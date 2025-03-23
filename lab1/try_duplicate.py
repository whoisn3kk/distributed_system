import grpc
import logging_pb2
import logging_pb2_grpc

def send_log(uuid_val, msg):
    channel = grpc.insecure_channel('localhost:50051')
    stub = logging_pb2_grpc.LoggingServiceStub(channel)
    request = logging_pb2.LogRequest(uuid=uuid_val, msg=msg)
    response = stub.Log(request)
    print(response.status)

test_uuid = "test-uuid-123"
send_log(test_uuid, "Перше повідомлення")
send_log(test_uuid, "Перше повідомлення")
