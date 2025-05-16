import grpc
from concurrent import futures
import time
import logging_pb2
import logging_pb2_grpc
import hazelcast
import logging
import argparse

# Налаштування логування
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(port)s - %(message)s')

# Global Hazelcast client and map (initialized in serve function)
hz_client = None
logs_map = None

class LoggingService(logging_pb2_grpc.LoggingServiceServicer):
    def Log(self, request, context):
        global logs_map
        if logs_map is None:
            logger.error("Hazelcast logs_map is not initialized.")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details("Hazelcast logs_map is not initialized.")
            return logging_pb2.LogResponse(status="Error: Hazelcast not initialized")

        try:
            # Use UUID as key, message as value. put() is idempotent.
            logs_map.put(request.uuid, request.msg) # Using blocking call for simplicity
            current_port = context.peer().split(':')[-1] # Hack to get current server port for logging
            extra_info = {'port': current_port}
            logger.info(f"Повідомлення збережено в Hazelcast: {request.uuid} -> {request.msg}", extra=extra_info)
            return logging_pb2.LogResponse(status="OK")
        except Exception as e:
            logger.error(f"Помилка при збереженні в Hazelcast: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Помилка Hazelcast: {str(e)}")
            return logging_pb2.LogResponse(status=f"Error: {str(e)}")

    def GetLogs(self, request, context):
        global logs_map
        if logs_map is None:
            logger.error("Hazelcast logs_map is not initialized.")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details("Hazelcast logs_map is not initialized.")
            return logging_pb2.LogsResponse(messages=[])
        
        try:
            # Retrieve all values from the map
            all_messages = list(logs_map.values())
            current_port = context.peer().split(':')[-1] # Hack
            extra_info = {'port': current_port}
            logger.info(f"Витягнуто {len(all_messages)} повідомлень з Hazelcast.", extra=extra_info)
            return logging_pb2.LogsResponse(messages=all_messages)
        except Exception as e:
            logger.error(f"Помилка при читанні з Hazelcast: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Помилка Hazelcast: {str(e)}")
            return logging_pb2.LogsResponse(messages=[])

def serve(port: int):
    global hz_client, logs_map

    # Initialize Hazelcast client
    # These addresses point to the host ports mapped to Hazelcast Docker containers
    try:
        hz_client = hazelcast.HazelcastClient(
            cluster_members=[
                "127.0.0.1:5701",
                "127.0.0.1:5702",
                "127.0.0.1:5703"
            ],
            cluster_name="dev" # Ensure this matches your hazelcast-docker.xml
        )
        # Get a distributed map
        logs_map = hz_client.get_map("logging_service_map").blocking()
        logger.info(f"Hazelcast client connected and map 'logging_service_map' obtained. Running on port {port}", extra={'port': str(port)})
    except Exception as e:
        logger.error(f"Не вдалося підключитися до Hazelcast або отримати карту: {e}", extra={'port': str(port)})
        # Optionally, exit if Hazelcast connection fails, or let gRPC calls fail.
        # For this demo, we'll let it run, and gRPC calls will indicate the map is not initialized.
        # return # Uncomment to exit if Hazelcast connection is critical for startup

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    logging_pb2_grpc.add_LoggingServiceServicer_to_server(LoggingService(), server)
    server.add_insecure_port(f'[::]:{port}')
    server.start()
    logger.info(f"Logging Service gRPC сервер запущено на порту {port}", extra={'port': str(port)})
    
    try:
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        logger.info("Зупинка сервера...", extra={'port': str(port)})
        server.stop(0)
        if hz_client:
            hz_client.shutdown()
            logger.info("Hazelcast client вимкнено.", extra={'port': str(port)})

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Logging Service with Hazelcast")
    parser.add_argument('--port', type=int, required=True, help='Port for the gRPC server')
    args = parser.parse_args()
    
    # Create a logger adapter to inject the port into log messages
    adapter = logging.LoggerAdapter(logger, {'port': str(args.port)})
    
    serve(args.port)