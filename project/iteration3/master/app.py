# ./iteration3/master/app.py
import asyncio
import logging
import os
import threading
import uuid
from collections import defaultdict

import httpx
# IMPORTANT: No longer importing BackgroundTasks
from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel

# --- Configuration & Globals ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - MASTER - %(levelname)s - %(message)s')
app = FastAPI(title="Master Replicated Log API - Iteration 3 (DEFINITIVE FIX)", version="3.2.0")

# --- In-memory data stores ---
messages_log = []
log_lock = threading.Lock()

current_sequence_number = 0
seq_num_lock = threading.Lock()

# --- Secondary Configuration ---
SECONDARY_URLS_STR = os.environ.get("SECONDARY_URLS", "")
SECONDARY_URLS = [url.strip() for url in SECONDARY_URLS_STR.split(',') if url.strip()]
SECONDARY_HEALTH = {url: "HEALTHY" for url in SECONDARY_URLS}

# --- ACK tracking using asyncio.Condition for robust waiting ---
ack_conditions = {}
ack_counts = defaultdict(int)

if SECONDARY_URLS:
    logging.info(f"Master initialized. Secondary URLs: {SECONDARY_URLS}")
else:
    logging.warning("Master initialized. No secondary URLs configured.")

# --- Pydantic Models ---
class MessageCreate(BaseModel):
    message: str
    w: int

class MessageDataInternal(BaseModel):
    id: str
    message: str
    seq_num: int

class MessageStored(BaseModel):
    id: str
    message: str
    seq_num: int

class PostResponse(BaseModel):
    message_id: str
    sequence_number: int

# --- Helper Functions ---
def get_next_sequence_number():
    global current_sequence_number
    with seq_num_lock:
        current_sequence_number += 1
        return current_sequence_number

async def persistent_replicate_task(secondary_url: str, message_data: MessageDataInternal):
    """
    Persistent task that retries sending a message to a secondary until it succeeds.
    It uses asyncio.Condition to notify the waiting client request.
    """
    message_id = message_data.id
    max_delay = 60
    retry_delay = 1

    condition = ack_conditions.get(message_id)
    if not condition:
        # This can happen if the client request timed out and cleaned up the condition
        # but the task is still running. It's safe to just log and exit.
        logging.warning(f"No condition found for message_id {message_id}. Request may have timed out. Task for {secondary_url} will continue silently.")
        # Create a dummy condition to avoid crashing if the original is gone
        condition = asyncio.Condition()
    
    while True:
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                logging.info(f"Attempting to replicate message ID {message_id} to {secondary_url}...")
                response = await client.post(f"{secondary_url}/replicate", json=message_data.model_dump())

                if response.status_code == status.HTTP_200_OK:
                    logging.info(f"Successfully replicated message ID {message_id} to {secondary_url}. ACK received.")
                    if SECONDARY_HEALTH.get(secondary_url) != "HEALTHY":
                        logging.warning(f"Secondary {secondary_url} is now HEALTHY.")
                        SECONDARY_HEALTH[secondary_url] = "HEALTHY"

                    # Notify waiting request using Condition
                    async with condition:
                        # Check if ack_count for this message still exists
                        if message_id in ack_counts:
                            ack_counts[message_id] += 1
                            condition.notify()

                    return # Exit loop on success

                elif response.status_code >= 500:
                     logging.error(f"Secondary {secondary_url} returned a server error {response.status_code} for msg {message_id}. Retrying...")
                     SECONDARY_HEALTH[secondary_url] = "UNHEALTHY"
                else:
                    logging.error(f"Unexpected status {response.status_code} from {secondary_url} for msg {message_id}. Retrying...")
                    SECONDARY_HEALTH[secondary_url] = "UNHEALTHY"

        except (httpx.RequestError, httpx.TimeoutException) as e:
            logging.error(f"Failed to connect to {secondary_url} for msg {message_id}: {e}. Retrying...")
            SECONDARY_HEALTH[secondary_url] = "UNHEALTHY"
        except Exception as e:
            logging.critical(f"Unexpected error in replication task for {secondary_url}, msg {message_id}: {e}", exc_info=True)
        
        await asyncio.sleep(retry_delay)
        retry_delay = min(retry_delay * 2, max_delay)


# --- API Endpoints ---
# NOTE: The function signature no longer includes `background_tasks: BackgroundTasks`
@app.post("/messages", response_model=PostResponse, status_code=status.HTTP_202_ACCEPTED)
async def append_message(payload: MessageCreate):
    if not (1 <= payload.w <= 1 + len(SECONDARY_URLS)):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Write concern 'w' must be between 1 and {1 + len(SECONDARY_URLS)}",
        )

    message_id = str(uuid.uuid4())
    seq_num = get_next_sequence_number()
    message_data = MessageDataInternal(id=message_id, message=payload.message, seq_num=seq_num)

    with log_lock:
        messages_log.append(message_data.model_dump())
    logging.info(f"Appended to master log: ID {message_id}, Seq {seq_num}, Msg '{payload.message}', w={payload.w}")

    ack_counts[message_id] = 1
    condition = asyncio.Condition()
    ack_conditions[message_id] = condition

    # --- THE FIX: Use asyncio.create_task to run tasks concurrently ---
    # These tasks start immediately, not after the response.
    for sec_url in SECONDARY_URLS:
        asyncio.create_task(persistent_replicate_task(sec_url, message_data))

    if payload.w == 1:
        # Clean up resources for this request as they won't be used for waiting
        ack_conditions.pop(message_id, None)
        # We can leave ack_counts entry as it's small
        return PostResponse(message_id=message_id, sequence_number=seq_num)

    client_timeout = 30.0
    try:
        async with condition:
            await asyncio.wait_for(
                condition.wait_for(lambda: ack_counts.get(message_id, 0) >= payload.w),
                timeout=client_timeout
            )
        logging.info(f"Met write concern w={payload.w} for message {message_id} with {ack_counts.get(message_id, 0)} ACKs.")
    except asyncio.TimeoutError:
        current_acks = ack_counts.get(message_id, 0)
        logging.error(f"Timeout waiting for w={payload.w} ACKs for message {message_id}. Have {current_acks} ACKs.")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Timeout: Failed to achieve write concern {payload.w}. Received {current_acks} ACKs of {payload.w} required.",
        )
    finally:
        # Clean up resources for this message_id to prevent memory leaks
        ack_conditions.pop(message_id, None)
        # It's also good practice to clean up the count
        ack_counts.pop(message_id, None)

    return PostResponse(message_id=message_id, sequence_number=seq_num)


@app.get("/messages", response_model=list[MessageStored])
async def get_messages_from_master():
    with log_lock:
        return [MessageStored(**msg) for msg in messages_log]


@app.get("/")
async def root_master():
    return {
        "message": "Master Replicated Log (Iteration 3 - DEFINITIVE FIX) is running",
        "secondaries": SECONDARY_URLS,
        "health": SECONDARY_HEALTH,
        "log_size": len(messages_log)
    }