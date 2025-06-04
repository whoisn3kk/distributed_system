from fastapi import FastAPI, HTTPException, status, BackgroundTasks
from pydantic import BaseModel
import httpx # For asynchronous HTTP calls to secondaries
import logging
import os
import threading # For sequence number lock
import uuid
import asyncio

# --- Configuration & Globals ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - MASTER - %(levelname)s - %(message)s')
app = FastAPI(title="Master Replicated Log API - Iteration 2", version="2.0.0")

messages_log = []  # Stores dicts: {'id': str, 'message': str, 'seq_num': int}
log_lock = threading.Lock() # For messages_log access (if modified outside main FastAPI thread, though mostly append)

current_sequence_number = 0
seq_num_lock = threading.Lock() # Critical for generating unique sequence numbers

SECONDARY_URLS_STR = os.environ.get("SECONDARY_URLS", "")
SECONDARY_URLS = [url.strip() for url in SECONDARY_URLS_STR.split(',') if url.strip()]

# In-memory tracking for ACKs for a given message_id for Iteration 2 & 3
# message_id -> asyncio.Event to signal client response
# message_id -> count of ACKs from secondaries
# message_id -> list of secondaries that have ACKed (for more complex logic later)
client_response_events = {} # message_id -> asyncio.Event()
message_secondary_acks = {} # message_id -> set of secondary_urls that ACKed

if SECONDARY_URLS:
    logging.info(f"Master initialized. Secondary URLs: {SECONDARY_URLS}")
else:
    logging.warning("Master initialized. No secondary URLs configured.")

# --- Pydantic Models ---
class MessageCreate(BaseModel):
    message: str
    w: int # Write concern

class MessageDataInternal(BaseModel): # For internal replication
    id: str
    message: str
    seq_num: int

class MessageStored(BaseModel): # For GET /messages response
    id: str
    message: str
    seq_num: int

class PostResponse(BaseModel):
    status: str
    message_id: str
    sequence_number: int

# --- Helper Functions ---
def get_next_sequence_number():
    global current_sequence_number
    with seq_num_lock:
        current_sequence_number += 1
        return current_sequence_number

async def replicate_to_secondary_task(secondary_url: str, message_data: MessageDataInternal):
    """
    Task to replicate a message to a single secondary and handle its ACK.
    This will be run for all secondaries.
    """
    message_id = message_data.id
    try:
        async with httpx.AsyncClient(timeout=10.0) as client: # 10s timeout for replication attempt
            logging.info(f"Replicating message ID {message_id} (seq: {message_data.seq_num}) to {secondary_url}...")
            response = await client.post(f"{secondary_url}/replicate", json=message_data.model_dump())
            
            if response.status_code == status.HTTP_200_OK:
                logging.info(f"Successfully replicated message ID {message_id} to {secondary_url}. ACK received.")
                
                # Record ACK from this secondary
                if message_id not in message_secondary_acks:
                     # This should not happen if initialized in append_message
                    message_secondary_acks[message_id] = set()
                message_secondary_acks[message_id].add(secondary_url)

                # Check if client_response_event for this message_id exists and if w condition met
                # The check for w and setting event is handled in append_message or a dedicated monitor
                # For Iteration 2, this function's main job is to attempt replication and record ACK.
                # The main request handler will decide when to respond based on total ACKs.
                return True # Indicates successful ACK
            else:
                logging.error(f"Failed to replicate message ID {message_id} to {secondary_url}. Status: {response.status_code}, Response: {response.text}")
                # Iteration 2 assumes "perfect link", so this part might not be hit often by network errors,
                # but server errors on secondary (500) could occur.
                # Iteration 3 will add retries here.
                return False
    except httpx.TimeoutException:
        logging.error(f"Timeout replicating message ID {message_id} to {secondary_url}.")
        return False
    except httpx.RequestError as e:
        logging.error(f"Request error replicating message ID {message_id} to {secondary_url}: {e}")
        return False
    except Exception as e:
        logging.error(f"Unexpected error replicating message ID {message_id} to {secondary_url}: {e}", exc_info=True)
        return False


async def monitor_acks_and_signal(message_id: str, write_concern: int, num_total_secondaries: int):
    """
    Monitors ACKs for a message and sets the client_response_event when write_concern is met.
    This is a simplified monitor for Iteration 2.
    """
    client_event = client_response_events.get(message_id)
    if not client_event:
        logging.error(f"No client event found for message_id {message_id} in monitor.")
        return

    # Master's local write is 1 ACK. Need w-1 from secondaries.
    needed_secondary_acks = write_concern - 1
    
    # Wait until enough ACKs are received or all replication attempts are done (simplified for now)
    # This loop is a basic way to poll. A more event-driven way would be signals from replication tasks.
    # For now, the main `append_message` will await tasks or use a timeout.
    # Let's refine: the replication tasks themselves will update a counter, and the main handler awaits an event.
    
    # This function is more conceptual for now. The logic is within append_message.
    # The main idea is: replication tasks update `message_secondary_acks[message_id]`.
    # A central place (or the main request handler) checks `len(message_secondary_acks[message_id])`.

    # For Iteration 2, let's assume `append_message` orchestrates the waiting.
    pass


# --- API Endpoints ---
@app.post("/messages", response_model=PostResponse, status_code=status.HTTP_201_CREATED)
async def append_message(payload: MessageCreate, background_tasks: BackgroundTasks):
    message_text = payload.message
    write_concern_w = payload.w
    message_id = str(uuid.uuid4())
    
    if write_concern_w <= 0:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Write concern 'w' must be positive.")
    if write_concern_w > 1 + len(SECONDARY_URLS):
         # It's possible to accept this and block, but good to inform client if it's impossible
        logging.warning(f"Requested w={write_concern_w} is greater than 1 (master) + {len(SECONDARY_URLS)} (available secondaries). Request might block indefinitely if secondaries don't become available.")


    seq_num = get_next_sequence_number()
    message_data_internal = MessageDataInternal(id=message_id, message=message_text, seq_num=seq_num)
    
    # 1. Store message locally on Master (counts as 1st ACK)
    with log_lock:
        messages_log.append(message_data_internal.model_dump()) # Store as dict
    logging.info(f"Appended to master log: ID {message_id}, Seq {seq_num}, Msg '{message_text}', w={write_concern_w}")

    # Initialize ACK tracking for this message
    message_secondary_acks[message_id] = set()
    client_response_event = asyncio.Event()
    client_response_events[message_id] = client_response_event

    # --- Replication Logic ---
    # Tasks for replicating to secondaries
    replication_tasks = []
    if SECONDARY_URLS:
        for sec_url in SECONDARY_URLS:
            # Create a task for each replication.
            # These tasks will update `message_secondary_acks` upon success.
            task = asyncio.create_task(replicate_to_secondary_task(sec_url, message_data_internal))
            replication_tasks.append(task)
    
    # --- Write Concern Logic ---
    if write_concern_w == 1:
        logging.info(f"w=1, responding to client for msg_id {message_id} immediately.")
        # Replication to all secondaries (if any) will continue in the background.
        # `asyncio.create_task` already makes them run in background.
        # We don't await them here for w=1.
        # Clean up event for this message_id if not awaited (or handle it if it gets set later)
        # For w=1, the event isn't strictly needed for client response, but good for consistency.
        # To ensure cleanup, we can remove it after responding for w=1.
        if message_id in client_response_events: # Should be true
             client_response_events.pop(message_id, None) 
        if message_id in message_secondary_acks: # Should be true
             # This will be populated by background tasks. For strictness in later iterations, may need a lock.
             pass # message_secondary_acks[message_id] 

        return PostResponse(status="message appended to master", message_id=message_id, sequence_number=seq_num)

    # For w > 1: Wait for (w-1) ACKs from secondaries
    # The client response is blocked until this condition is met or a timeout occurs.
    # Total ACKs needed = w. Master is 1. So, w-1 from secondaries.
    needed_secondary_acks = write_concern_w - 1
    
    if needed_secondary_acks == 0: # This implies w=1, already handled. But as a safeguard.
        logging.info(f"w={write_concern_w} (effectively 1 as master is an ACK), responding to client for msg_id {message_id}.")
        if message_id in client_response_events: client_response_events.pop(message_id, None)
        return PostResponse(status="message appended to master", message_id=message_id, sequence_number=seq_num)

    # Wait for replication tasks to complete and check ACK count
    # We need to wait until `len(message_secondary_acks[message_id]) >= needed_secondary_acks`
    # This requires replication tasks to signal or update shared state reliably.
    
    # A simple way: periodically check or use an asyncio.Condition or rely on the event.
    # Let's make the replication tasks set the main client_response_event once global ACK count for *this msg_id* hits `needed_secondary_acks`.
    # This requires `replicate_to_secondary_task` to access and update shared ACK counts and check `w`.
    # For better encapsulation, tasks can return, and this function aggregates.

    # Refined logic for waiting for w ACKs:
    # Each `replicate_to_secondary_task` adds to `message_secondary_acks[message_id]`.
    # We need a way for this main handler to be notified.
    # Let's try to await the tasks and count successful ones until `w-1` is reached.

    successful_acks_from_secondaries = 0
    if replication_tasks:
        # Create a new event that signals when enough ACKs are received for *this specific request*
        enough_acks_event = asyncio.Event()

        async def await_ack_and_check(task: asyncio.Task):
            nonlocal successful_acks_from_secondaries
            try:
                success = await task # `replicate_to_secondary_task` returns True on ACK
                if success:
                    # This count is local to this request handler's processing of ACKs.
                    # `message_secondary_acks[message_id]` is the global view.
                    # For simplicity here, rely on the task return.
                    successful_acks_from_secondaries +=1
                    if successful_acks_from_secondaries >= needed_secondary_acks:
                        enough_acks_event.set()
            except Exception as e:
                logging.error(f"Error processing ACK for {message_id}: {e}")

        # Create tasks that will set the enough_acks_event
        ack_waiter_tasks = [asyncio.create_task(await_ack_and_check(task)) for task in replication_tasks]
        
        try:
            # Wait for the event to be set, or a timeout
            # Timeout needs to be sensible, e.g., 30 seconds.
            # If timeout occurs, client gets an error, but replication might still complete for other secondaries.
            await asyncio.wait_for(enough_acks_event.wait(), timeout=30.0) # Example timeout
            logging.info(f"Met write concern w={write_concern_w} for message {message_id} ({successful_acks_from_secondaries} secondary ACKs). Responding to client.")
            
        except asyncio.TimeoutError:
            logging.error(f"Timeout waiting for w={write_concern_w} ACKs for message {message_id}. Have {successful_acks_from_secondaries} secondary ACKs.")
            # Clean up: cancel pending ack_waiter_tasks and underlying replication_tasks if necessary
            for t in ack_waiter_tasks: t.cancel()
            # Underlying replication_tasks (in `replication_tasks`) will continue unless explicitly cancelled.
            # For Iteration 2, we assume perfect link, so this timeout might be more about secondary processing time.
            # Iteration 3 will handle down nodes.
            raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, 
                                detail=f"Timeout: Failed to achieve write concern {write_concern_w}. Received {1+successful_acks_from_secondaries} ACKs.")
        finally:
            # Ensure all original replication tasks are awaited if not cancelled to prevent "task never awaited"
            # This is tricky if some are still running and we timed out.
            # For now, background tasks for remaining replications will continue.
            # Iteration 3 will need more robust handling of ongoing/failed replications.
             if message_id in client_response_events: client_response_events.pop(message_id, None) # Clean up event
             # `message_secondary_acks` will reflect the actual state.
    elif needed_secondary_acks > 0 : # No secondaries configured, but w > 1
        logging.error(f"Write concern w={write_concern_w} cannot be met as there are no secondaries.")
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                            detail=f"Write concern {write_concern_w} cannot be met: no secondaries available.")


    # If we reached here for w > 1, it means enough ACKs were received.
    # Any remaining replication tasks (beyond w-1) continue in the background.
    return PostResponse(status=f"message appended and replicated with w={write_concern_w}", 
                        message_id=message_id, sequence_number=seq_num)


@app.get("/messages", response_model=list[MessageStored])
async def get_messages_from_master():
    with log_lock: # Ensure read is safe if writes can happen from other threads (though FastAPI is mostly single-threaded per request)
        return [MessageStored(**msg) for msg in messages_log]

@app.get("/")
async def root_master():
    return {"message": "Master Replicated Log (Iteration 2) is running", "secondaries": SECONDARY_URLS}