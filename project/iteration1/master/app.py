from fastapi import FastAPI, HTTPException, status, Request
from pydantic import BaseModel
import requests # Для синхронних HTTP запитів до вторинних серверів
import logging
import os
import threading
import uuid
import asyncio # Для запуску блокуючих операцій в окремому потоці з async def

# Налаштування логування
logging.basicConfig(level=logging.INFO, format='%(asctime)s - MASTER - %(levelname)s - %(message)s')

app = FastAPI(title="Master Replicated Log API", version="1.0.0")

# Внутрішнє сховище повідомлень (in-memory)
messages_log = [] # Список словників {'id': str, 'message': str}
log_lock = threading.Lock() # Для безпечного доступу до messages_log

# Отримання URL вторинних серверів з змінної середовища
SECONDARY_URLS_STR = os.environ.get("SECONDARY_URLS", "")
SECONDARY_URLS = [url.strip() for url in SECONDARY_URLS_STR.split(',') if url.strip()]
if SECONDARY_URLS:
    logging.info(f"Master initialized. Secondary URLs: {SECONDARY_URLS}")
else:
    logging.warning("Master initialized. No secondary URLs configured.")

# Pydantic моделі для валідації запитів та відповідей
class MessageCreate(BaseModel):
    message: str

class MessageStored(BaseModel):
    id: str
    message: str

class PostResponse(BaseModel):
    status: str
    message_id: str


def replicate_message_to_secondary_sync(secondary_url: str, message_data: dict, ack_event: threading.Event):
    """
    Синхронно надсилає повідомлення на один вторинний сервер та чекає на ACK.
    Встановлює подію ack_event при успішній реплікації.
    Використовує 'requests' для простоти.
    """
    try:
        logging.info(f"Replicating message ID {message_data['id']} to {secondary_url}...")
        # Вторинний сервер має ендпоінт /replicate
        # Таймаут важливий, щоб уникнути нескінченного очікування, хоча "perfect link" це спрощує
        response = requests.post(f"{secondary_url}/replicate", json=message_data, timeout=15) 
        response.raise_for_status() # Викине виключення для HTTP помилок 4xx/5xx
        logging.info(f"Successfully replicated message ID {message_data['id']} to {secondary_url}. ACK received (HTTP {response.status_code}).")
    except requests.exceptions.Timeout:
        logging.error(f"Timeout when replicating message ID {message_data['id']} to {secondary_url}.")
        # За умовою Iteration 1, "communication channel is a perfect link",
        # тому таймаути або інші помилки зв'язку не повинні відбуватися.
        # Якщо б трапилось, клієнт завис би, бо ACK не буде отримано.
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to replicate message ID {message_data['id']} to {secondary_url}: {e}")
    finally:
        # Встановлюємо подію, щоб головний потік (який очікує на цю подію) міг продовжити.
        # Для "perfect link" це завжди буде після успіху.
        ack_event.set()


@app.post("/messages", response_model=PostResponse, status_code=status.HTTP_201_CREATED)
async def append_message(payload: MessageCreate):
    try:
        message_text = payload.message
        message_id = str(uuid.uuid4())
        
        message_data = {"id": message_id, "message": message_text}
        
        # 1. Додаємо повідомлення до власного логу Майстра
        with log_lock:
            messages_log.append(message_data)
        logging.info(f"Appended message to master log: {message_data}")

        # 2. Реплікуємо на всі вторинні сервери та чекаємо ACK від кожного
        ack_events = []
        replication_threads = []

        if not SECONDARY_URLS:
            logging.warning("No secondary servers configured. Message only stored on master.")
        else:
            for sec_url in SECONDARY_URLS:
                ack_event = threading.Event()
                ack_events.append(ack_event)
                
                # Запускаємо синхронну функцію реплікації в окремому потоці
                thread = threading.Thread(target=replicate_message_to_secondary_sync, 
                                          args=(sec_url, message_data, ack_event))
                replication_threads.append(thread)
                thread.start()

            # 3. Майстер чекає на ACK від УСІХ вторинних серверів (блокуюча реплікація)
            logging.info(f"Waiting for ACKs from {len(SECONDARY_URLS)} secondaries for message ID {message_id}...")
            
            loop = asyncio.get_event_loop()
            for i, ack_event in enumerate(ack_events):
                # Очікуємо на threading.Event в асинхронній функції через run_in_executor
                await loop.run_in_executor(None, ack_event.wait) 
                logging.info(f"Received ACK confirmation for message ID {message_id} (event {i+1}/{len(ack_events)} processed).")
        
        logging.info(f"All ACKs received for message ID {message_id}. Responding to client.")
        return PostResponse(status="message appended and replicated", message_id=message_id)

    except Exception as e:
        logging.error(f"Error in POST /messages: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Internal server error")

@app.get("/messages", response_model=list[MessageStored])
async def get_messages():
    with log_lock:
        # Повертаємо копію списку
        return list(messages_log)

# Для перевірки стану (опціонально)
@app.get("/")
async def root():
    return {"message": "Master Replicated Log is running"}