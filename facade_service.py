import logging
from fastapi import FastAPI, HTTPException
import uuid
import grpc
import requests
from tenacity import retry, wait_fixed, stop_after_attempt, before_sleep_log
import logging_pb2
import logging_pb2_grpc

app = FastAPI()

# Налаштування логування
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def get_grpc_stub():
    channel = grpc.insecure_channel('localhost:50051')
    stub = logging_pb2_grpc.LoggingServiceStub(channel)
    return stub

# Декоратор retry із логуванням перед наступною спробою
@retry(wait=wait_fixed(2), stop=stop_after_attempt(3), before_sleep=before_sleep_log(logger, logging.WARNING))
def grpc_log_message(message_id, msg):
    stub = get_grpc_stub()
    request = logging_pb2.LogRequest(uuid=message_id, msg=msg)
    response = stub.Log(request)
    return response

@retry(wait=wait_fixed(2), stop=stop_after_attempt(3), before_sleep=before_sleep_log(logger, logging.WARNING))
def grpc_get_logs():
    stub = get_grpc_stub()
    empty = logging_pb2.Empty()
    response = stub.GetLogs(empty)
    return response

# URL для messages-service (HTTP)
MESSAGES_SERVICE_URL = "http://localhost:8002/messages"

@app.post("/message")
async def post_message(item: dict):
    if "msg" not in item:
        raise HTTPException(status_code=400, detail="Відсутнє поле 'msg'")
    message = item["msg"]
    message_id = str(uuid.uuid4())
    try:
        # Виклик logging-service через gRPC із retry
        grpc_log_message(message_id, message)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Помилка при відправці повідомлення до logging-service: {e}")
    return {"uuid": message_id, "msg": message}

@app.get("/message")
async def get_messages():
    try:
        grpc_response = grpc_get_logs()
        logs = grpc_response.messages  # Список повідомлень
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Помилка при отриманні логів: {e}")
    # Отримання відповіді від messages-service (HTTP GET)
    try:
        r = requests.get(MESSAGES_SERVICE_URL)
        messages_service_response = r.text
    except Exception as e:
        messages_service_response = "Помилка з'єднання з messages-service"
    # Об'єднання відповідей
    combined = " ".join(logs) + " " + messages_service_response
    return {"combined": combined}
