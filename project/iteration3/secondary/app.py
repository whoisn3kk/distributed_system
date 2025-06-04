import asyncio
import logging
import os
import threading
import time

from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel

# --- Configuration & Globals ---
HOSTNAME_ENV = os.environ.get("HOSTNAME", "secondary-unknown")
logging.basicConfig(level=logging.INFO, format=f'%(asctime)s - SECONDARY ({HOSTNAME_ENV}) - %(levelname)s - %(message)s')

app = FastAPI(title=f"Secondary Replicated Log API ({HOSTNAME_ENV}) - Iteration 3", version="3.0.0")

# --- In-memory data stores and state ---
replicated_messages_log = [] # The final, ordered log: [{'id': str, ...}]
processed_message_ids = set() # For fast deduplication check
log_lock = threading.Lock()

# --- Total Order and Buffering Logic ---
expected_seq_num = 1
message_buffer = {} # Stores out-of-order messages: {seq_num: payload}

# --- Simulation Settings ---
ARTIFICIAL_DELAY = float(os.environ.get("ARTIFICIAL_DELAY", 0))
# To test total order and retries: fail on a specific sequence number once.
FAIL_ON_SEQ_NUM = int(os.environ.get("FAIL_ON_SEQ_NUM", -1))
failed_once = False # State to ensure we only fail once

# --- Pydantic Models ---
class ReplicatePayload(BaseModel):
    id: str
    message: str
    seq_num: int

class MessageStored(BaseModel):
    id: str
    message: str
    seq_num: int

# --- Helper Functions ---
def _process_buffered_messages():
    """
    Processes messages from the buffer that are now in sequence.
    This function MUST be called within a `log_lock`.
    """
    global expected_seq_num
    while expected_seq_num in message_buffer:
        msg_to_process = message_buffer.pop(expected_seq_num)
        logging.info(f"Processing buffered message Seq {msg_to_process.seq_num}...")
        replicated_messages_log.append(msg_to_process.model_dump())
        processed_message_ids.add(msg_to_process.id)
        expected_seq_num += 1
        logging.info(f"Log size: {len(replicated_messages_log)}. Next expected Seq: {expected_seq_num}")

# --- API Endpoints ---
@app.post("/replicate", status_code=status.HTTP_200_OK)
async def replicate_on_secondary(payload: ReplicatePayload):
    global failed_once, expected_seq_num
    logging.info(f"Received replication request: ID {payload.id}, Seq {payload.seq_num}")

    # --- Simulate failure for testing ---
    if payload.seq_num == FAIL_ON_SEQ_NUM and not failed_once:
        failed_once = True
        logging.error(f"Simulating internal server error for Seq {payload.seq_num} as requested.")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Simulated server failure")

    # --- Simulate network/processing delay ---
    if ARTIFICIAL_DELAY > 0:
        await asyncio.sleep(ARTIFICIAL_DELAY)

    with log_lock:
        # --- 1. Deduplication ---
        if payload.id in processed_message_ids:
            logging.warning(f"Message ID {payload.id} (Seq {payload.seq_num}) already processed. Ignoring duplicate, sending ACK.")
            return {"status": "message already replicated (duplicate)"}
        if payload.seq_num in message_buffer:
             logging.warning(f"Message Seq {payload.seq_num} already in buffer. Ignoring duplicate, sending ACK.")
             return {"status": "message already buffered (duplicate)"}

        # --- 2. Total Order Logic ---
        if payload.seq_num == expected_seq_num:
            logging.info(f"Received expected message Seq {payload.seq_num}. Appending to log.")
            replicated_messages_log.append(payload.model_dump())
            processed_message_ids.add(payload.id)
            expected_seq_num += 1
            logging.info(f"Log size: {len(replicated_messages_log)}. Next expected Seq: {expected_seq_num}")
            
            # After appending, check buffer for next messages
            _process_buffered_messages()
        elif payload.seq_num > expected_seq_num:
            logging.warning(f"Received out-of-order message Seq {payload.seq_num}. Expected {expected_seq_num}. Buffering.")
            message_buffer[payload.seq_num] = payload
        else: # payload.seq_num < expected_seq_num
            logging.warning(f"Received old message Seq {payload.seq_num} (already processed). Ignoring.")

    # Always return 200 OK if message is accepted (processed or buffered)
    # The master only needs to know the message was received safely.
    return {"status": "message received"}


@app.get("/messages", response_model=list[MessageStored])
async def get_replicated_messages_from_secondary():
    with log_lock:
        # Return a sorted copy to ensure consistent output, even though our log should be ordered
        sorted_log = sorted(replicated_messages_log, key=lambda x: x['seq_num'])
        return [MessageStored(**msg) for msg in sorted_log]

@app.get("/")
async def root_secondary():
    with log_lock:
        return {
            "message": f"Secondary Replicated Log ({HOSTNAME_ENV}) (Iteration 3) is running",
            "log_size": len(replicated_messages_log),
            "buffer_size": len(message_buffer),
            "expected_seq_num": expected_seq_num
        }