from fastapi import FastAPI, HTTPException, Request, status
from pydantic import BaseModel
import logging
import os
import time
import threading # For log_lock
import asyncio

# --- Configuration & Globals ---
HOSTNAME_ENV = os.environ.get("HOSTNAME", "secondary-unknown")
logging.basicConfig(level=logging.INFO, 
                    format=f'%(asctime)s - SECONDARY ({HOSTNAME_ENV}) - %(levelname)s - %(message)s')

app = FastAPI(title=f"Secondary Replicated Log API ({HOSTNAME_ENV}) - Iteration 2", version="2.0.0")

replicated_messages_log = [] # Stores dicts: {'id': str, 'message': str, 'seq_num': int}
processed_message_ids = set() # For deduplication
log_lock = threading.Lock() 

ARTIFICIAL_DELAY = float(os.environ.get("ARTIFICIAL_DELAY", 0))

# For Iteration 3 total order:
# expected_sequence_number = 0 
# message_buffer = {} # seq_num -> message_data

# --- Pydantic Models ---
class ReplicatePayload(BaseModel): # Received from Master
    id: str
    message: str
    seq_num: int

class ReplicateResponse(BaseModel):
    status: str
    id: str
    seq_num: int

class MessageStored(BaseModel): # For GET /messages response
    id: str
    message: str
    seq_num: int

# --- API Endpoints ---
@app.post("/replicate", response_model=ReplicateResponse, status_code=status.HTTP_200_OK)
async def replicate_on_secondary(payload: ReplicatePayload):
    logging.info(f"Received replication request: ID {payload.id}, Seq {payload.seq_num}, Msg '{payload.message}'")

    if ARTIFICIAL_DELAY > 0:
        logging.info(f"Artificial delay of {ARTIFICIAL_DELAY} seconds started...")
        await asyncio.sleep(ARTIFICIAL_DELAY)
        logging.info(f"Artificial delay finished.")

    with log_lock:
        # Deduplication
        if payload.id in processed_message_ids:
            logging.warning(f"Message ID {payload.id} (Seq {payload.seq_num}) already processed. Ignoring duplicate, sending ACK.")
            # Still send ACK for idempotency as master expects it.
            return ReplicateResponse(status="message already replicated (duplicate)", id=payload.id, seq_num=payload.seq_num)

        # For Iteration 2, direct append. Total order handling will be more robust in Iteration 3.
        # Here, we assume master sends them in order and network is perfect.
        replicated_messages_log.append(payload.model_dump())
        processed_message_ids.add(payload.id)
        
        # Sort by sequence number for consistent GET /messages,
        # though robust ordering upon receive is for Iteration 3
        replicated_messages_log.sort(key=lambda x: x['seq_num'])

        logging.info(f"Message ID {payload.id} (Seq {payload.seq_num}) appended to secondary log. Log size: {len(replicated_messages_log)}")
    
    return ReplicateResponse(status="message replicated", id=payload.id, seq_num=payload.seq_num)

@app.get("/messages", response_model=list[MessageStored])
async def get_replicated_messages_from_secondary():
    with log_lock:
        # Ensure items are MessageStored instances for response model validation
        return [MessageStored(**msg) for msg in replicated_messages_log]

@app.get("/")
async def root_secondary():
    return {"message": f"Secondary Replicated Log ({HOSTNAME_ENV}) (Iteration 2) is running", "delay": ARTIFICIAL_DELAY, "log_size": len(replicated_messages_log)}