import grpc
from concurrent import futures
import time
import logging_pb2
import logging_pb2_grpc

# Внутрішнє сховище повідомлень: ключ – uuid, значення – текст повідомлення
storage = {}

class LoggingService(logging_pb2_grpc.LoggingServiceServicer):
    def Log(self, request, context):
        if request.uuid not in storage:
            storage[request.uuid] = request.msg
            print(f"Отримано повідомлення: {request.uuid} -> {request.msg}")
        else:
            print(f"Отримано дубльоване повідомлення: {request.uuid}")
        return logging_pb2.LogResponse(status="OK")
    
    def GetLogs(self, request, context):
        msgs = list(storage.values())
        return logging_pb2.LogsResponse(messages=msgs)

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    logging_pb2_grpc.add_LoggingServiceServicer_to_server(LoggingService(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    print("Запущено Logging Service gRPC сервер на порту 50051")
    try:
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        server.stop(0)

if __name__ == '__main__':
    serve()
