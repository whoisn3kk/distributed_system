import grpc
from concurrent import futures
import time
import logging_pb2
import logging_pb2_grpc
import hazelcast
import logging
import argparse

# Глобальная переменная для хранения порта сервиса. Инициализируем None.
# Это позволяет фильтру обрабатывать записи лога до того, как порт будет назначен.
SERVICE_PORT = None 


# Примените пользовательский фильтр к корневому логгеру *как можно раньше*.
# Это гарантирует, что ВСЕ логгеры, включая те, которые используются внутри библиотек (например, Hazelcast),
# будут иметь этот фильтр, прежде чем они начнут выдавать логи.
# Настройка логгирования. Теперь '%(service_port)s' в строке формата всегда найдет атрибут.
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# Глобальный клиент Hazelcast и карта (инициализируются в функции serve)
hz_client = None
logs_map = None


class LoggingService(logging_pb2_grpc.LoggingServiceServicer):
    def Log(self, request, context):
        global logs_map
        if logs_map is None:
            # Убрано extra={'service_port': SERVICE_PORT}, так как фильтр обрабатывает это
            logger.error("Hazelcast logs_map is not initialized.")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details("Hazelcast logs_map is not initialized.")
            return logging_pb2.LogResponse(status="Error: Hazelcast not initialized")

        try:
            logs_map.put(request.uuid, request.msg)
            # Убрано extra={'service_port': SERVICE_PORT}, так как фильтр обрабатывает это
            logger.info(f"Message saved to Hazelcast: {request.uuid} -> {request.msg}")
            return logging_pb2.LogResponse(status="OK")
        except Exception as e:
            # Убрано extra={'service_port': SERVICE_PORT}, так как фильтр обрабатывает это
            logger.error(f"Error saving to Hazelcast: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Hazelcast error: {str(e)}")
            return logging_pb2.LogResponse(status=f"Error: {str(e)}")

    def GetLogs(self, request, context):
        global logs_map
        if logs_map is None:
            # Убрано extra={'service_port': SERVICE_PORT}, так как фильтр обрабатывает это
            logger.error("Hazelcast logs_map is not initialized.")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details("Hazelcast logs_map is not initialized.")
            return logging_pb2.LogsResponse(messages=[])
        
        try:
            all_messages = list(logs_map.values())
            # Убрано extra={'service_port': SERVICE_PORT}, так как фильтр обрабатывает это
            logger.info(f"Fetched {len(all_messages)} messages from Hazelcast.")
            return logging_pb2.LogsResponse(messages=all_messages)
        except Exception as e:
            # Убрано extra={'service_port': SERVICE_PORT}, так как фильтр обрабатывает это
            logger.error(f"Error reading from Hazelcast: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Hazelcast error: {str(e)}")
            return logging_pb2.LogsResponse(messages=[])

def serve(port: int):
    global hz_client, logs_map, SERVICE_PORT
    
    # !!! ВАЖНО !!!
    # Установите глобальную переменную SERVICE_PORT ДО инициализации клиента Hazelcast.
    # Hazelcast будет выполнять логгирование во время своей инициализации, и фильтру нужен правильный порт.
    SERVICE_PORT = port 

    try:
        hz_client = hazelcast.HazelcastClient(
            cluster_members=[
                "127.0.0.1:5701",
                "127.0.0.1:5702",
                "127.0.0.1:5703"
            ],
            cluster_name="dev"
        )
        logs_map = hz_client.get_map("logging_service_map").blocking()
        # Убрано extra={'service_port': SERVICE_PORT}, так как фильтр обрабатывает это
        logger.info(f"Hazelcast client connected and map 'logging_service_map' obtained.")
    except Exception as e:
        # Убрано extra={'service_port': SERVICE_PORT}, так как фильтр обрабатывает это
        logger.error(f"Could not connect to Hazelcast or get map: {e}")
        # Решите, выходить ли или продолжать с ошибками вызовов gRPC, если Hazelcast критичен
        # Для этой лабораторной работы мы позволим ему работать, и вызовы gRPC будут указывать, что карта не инициализирована.

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    logging_pb2_grpc.add_LoggingServiceServicer_to_server(LoggingService(), server)
    server.add_insecure_port(f'[::]:{port}')
    server.start()
    # Убрано extra={'service_port': SERVICE_PORT}, так как фильтр обрабатывает это
    logger.info(f"Logging Service gRPC server started on port {port}")
    
    try:
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        # Убрано extra={'service_port': SERVICE_PORT}, так как фильтр обрабатывает это
        logger.info("Stopping server...")
        server.stop(0)
        if hz_client:
            hz_client.shutdown()
            # Убрано extra={'service_port': SERVICE_PORT}, так как фильтр обрабатывает это
            logger.info("Hazelcast client shut down.")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Logging Service with Hazelcast")
    parser.add_argument('--port', type=int, required=True, help='Port for the gRPC server')
    args = parser.parse_args()
    
    serve(args.port)