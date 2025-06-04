from fastapi import FastAPI, HTTPException, Request, status
from pydantic import BaseModel
import logging
import os
import time
import threading
import asyncio # Для asyncio.sleep

# Налаштування логування
# HOSTNAME буде встановлено в docker-compose для розрізнення логів від різних secondary
HOSTNAME_ENV = os.environ.get("HOSTNAME", "secondary-unknown")
logging.basicConfig(level=logging.INFO, 
                    format=f'%(asctime)s - SECONDARY ({HOSTNAME_ENV}) - %(levelname)s - %(message)s')

app = FastAPI(title=f"Secondary Replicated Log API ({HOSTNAME_ENV})", version="1.0.0")

# Внутрішнє сховище реплікованих повідомлень (in-memory)
replicated_messages_log = [] # Список словників {'id': str, 'message': str}
log_lock = threading.Lock() # Для безпечного доступу

# Штучна затримка для тестування блокуючої реплікації
ARTIFICIAL_DELAY = float(os.environ.get("ARTIFICIAL_DELAY", 0))

# Pydantic моделі
class ReplicatePayload(BaseModel):
    id: str
    message: str

class ReplicateResponse(BaseModel):
    status: str
    id: str

class MessageStored(BaseModel): # Така ж як у Майстра
    id: str
    message: str


@app.post("/replicate", response_model=ReplicateResponse, status_code=status.HTTP_200_OK)
async def replicate_message(payload: ReplicatePayload):
    try:
        logging.info(f"Received replication request for message: ID {payload.id}, Message: '{payload.message}'")

        # Імітація затримки на вторинному сервері
        if ARTIFICIAL_DELAY > 0:
            logging.info(f"Artificial delay of {ARTIFICIAL_DELAY} seconds started...")
            await asyncio.sleep(ARTIFICIAL_DELAY) # Використовуємо asyncio.sleep в async функції
            logging.info(f"Artificial delay finished.")

        with log_lock:
            # Проста перевірка на дублікати за ID (корисно, хоча не строго вимагається в Iteration 1)
            if any(msg['id'] == payload.id for msg in replicated_messages_log):
                logging.warning(f"Message ID {payload.id} already exists. Ignoring duplicate, but sending ACK.")
            else:
                replicated_messages_log.append(payload.model_dump()) # Зберігаємо як dict
                logging.info(f"Message ID {payload.id} appended to secondary log.")
        
        # Надсилаємо ACK (успішний HTTP статус та тіло відповіді)
        return ReplicateResponse(status="message replicated", id=payload.id)

    except Exception as e:
        logging.error(f"Error in POST /replicate: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
                            detail="Internal server error during replication on secondary")

@app.get("/messages", response_model=list[MessageStored])
async def get_replicated_messages_from_secondary():
    with log_lock:
        return list(replicated_messages_log)

# Для перевірки стану (опціонально)
@app.get("/")
async def root_secondary():
    return {"message": f"Secondary Replicated Log ({HOSTNAME_ENV}) is running", "delay": ARTIFICIAL_DELAY}