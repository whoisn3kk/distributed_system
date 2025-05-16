import logging
from fastapi import FastAPI, HTTPException
import uuid
import grpc
import requests
from tenacity import retry, wait_fixed, stop_after_attempt, before_sleep_log, retry_if_exception_type
import random
import logging_pb2 # Make sure this file is in the same directory
import logging_pb2_grpc # Make sure this file is in the same directory
import uvicorn

app = FastAPI()

# Налаштування логування
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# Global cache for service addresses
SERVICE_ADDRESSES = {}
CONFIG_SERVER_URL = "http://localhost:8000/services" # URL of your config_server

def refresh_service_addresses(service_name: str) -> list:
    """Fetches and updates service addresses from config server, returns current list."""
    global SERVICE_ADDRESSES
    try:
        url = f"{CONFIG_SERVER_URL}/{service_name}"
        logger.info(f"Fetching addresses for '{service_name}' from {url}")
        response = requests.get(url, timeout=3) # Short timeout for config server
        response.raise_for_status()
        data = response.json()
        addresses = data.get("addresses", [])
        SERVICE_ADDRESSES[service_name] = addresses
        logger.info(f"Refreshed addresses for {service_name}: {addresses}")
        if not addresses:
             logger.warning(f"No addresses returned from config server for '{service_name}'.")
        return addresses
    except requests.RequestException as e:
        logger.error(f"Failed to fetch addresses for {service_name} from config server: {e}. Using cached: {SERVICE_ADDRESSES.get(service_name, [])}")
        # Return cached version if available, otherwise empty list
        return SERVICE_ADDRESSES.get(service_name, [])
    except Exception as e: # Catch any other error like JSON decode
        logger.error(f"Unexpected error fetching addresses for {service_name}: {e}. Using cached: {SERVICE_ADDRESSES.get(service_name, [])}")
        return SERVICE_ADDRESSES.get(service_name, [])


@app.on_event("startup")
async def startup_event():
    logger.info("Facade Service starting up. Initializing service addresses...")
    refresh_service_addresses("logging-service")
    refresh_service_addresses("messages-service")


def execute_grpc_call_with_failover(service_name: str, call_func):
    """
    Executes a gRPC call to a randomly selected instance of service_name.
    Handles failover by trying other instances if one fails.
    Refreshes addresses from config server if no instances are available or all fail.
    """
    current_addresses = SERVICE_ADDRESSES.get(service_name, [])
    
    if not current_addresses:
        logger.warning(f"No cached addresses for {service_name}, attempting refresh from config server.")
        current_addresses = refresh_service_addresses(service_name)

    if not current_addresses:
        logger.error(f"Still no addresses available for {service_name} after refresh.")
        raise HTTPException(status_code=503, detail=f"No instances available for service '{service_name}'.")

    # Create a shuffled list of addresses to try
    shuffled_addresses = list(current_addresses) # Make a copy
    random.shuffle(shuffled_addresses)
    
    last_exception = None

    for address in shuffled_addresses:
        channel = None # Ensure channel is defined for finally block
        try:
            logger.info(f"Attempting gRPC call to {service_name} at {address}")
            channel = grpc.insecure_channel(address)
            # Check channel readiness with a short timeout (e.g., 1 second)
            grpc.channel_ready_future(channel).result(timeout=1.0) 
            stub = logging_pb2_grpc.LoggingServiceStub(channel)
            response = call_func(stub) # Let the call_func apply its own RPC timeout
            logger.info(f"Successfully connected and called {service_name} at {address}")
            return response # Success
        except grpc.FutureTimeoutError: # channel.result(timeout) failed
            logger.warning(f"Timeout connecting to {service_name} instance at {address}.")
            last_exception = HTTPException(status_code=504, detail=f"Timeout connecting to {service_name} instance at {address}")
        except grpc.RpcError as e:
            logger.warning(f"gRPC call to {service_name} instance at {address} failed: {e.code()} - {e.details()}")
            # Specific handling for UNAVAILABLE, often means server is down or network issue
            if e.code() == grpc.StatusCode.UNAVAILABLE:
                last_exception = HTTPException(status_code=503, detail=f"Service {service_name} at {address} unavailable: {e.details()}")
            else:
                last_exception = HTTPException(status_code=500, detail=f"gRPC error with {service_name} at {address}: {e.details()}")
        except Exception as e:
            logger.error(f"Unexpected error with {service_name} at {address}: {type(e).__name__} - {e}")
            last_exception = HTTPException(status_code=500, detail=f"Unexpected error with {service_name} at {address}: {str(e)}")
        finally:
            if channel:
                channel.close()
    
    # If all instances failed, try to refresh config and raise the last known exception
    logger.error(f"All instances of {service_name} failed. Last error: {last_exception.detail if last_exception else 'Unknown'}. Triggering address refresh.")
    refresh_service_addresses(service_name) # Refresh for next time
    if last_exception:
        raise last_exception
    else: # Should not be reached if current_addresses was not empty
        raise HTTPException(status_code=503, detail=f"All instances of {service_name} failed, no specific error captured.")


# Tenacity for retrying the entire operation (including instance failover)
@retry(
    wait=wait_fixed(1), # Wait 1 second between retries
    stop=stop_after_attempt(3), # Attempt the operation up to 3 times
    before_sleep=before_sleep_log(logger, logging.WARNING), # Log before retrying
    retry=retry_if_exception_type((HTTPException, grpc.RpcError)) # Retry on these exceptions
)
async def resilient_grpc_log_message(message_id: str, msg: str):
    def call_logic(stub):
        request = logging_pb2.LogRequest(uuid=message_id, msg=msg)
        # Timeout for the specific RPC call
        return stub.Log(request, timeout=2.0) 
    return execute_grpc_call_with_failover("logging-service", call_logic)

@retry(
    wait=wait_fixed(1),
    stop=stop_after_attempt(3),
    before_sleep=before_sleep_log(logger, logging.WARNING),
    retry=retry_if_exception_type((HTTPException, grpc.RpcError))
)
async def resilient_grpc_get_logs():
    def call_logic(stub):
        empty = logging_pb2.Empty()
        # Potentially longer timeout if GetLogs can return a lot of data
        return stub.GetLogs(empty, timeout=5.0)
    return execute_grpc_call_with_failover("logging-service", call_logic)


@app.post("/message")
async def post_message(item: dict):
    if "msg" not in item:
        raise HTTPException(status_code=400, detail="Відсутнє поле 'msg'")
    message = item["msg"]
    message_id = str(uuid.uuid4())
    
    try:
        logger.info(f"POST /message: UUID={message_id}, Msg='{message}'. Sending to logging-service...")
        await resilient_grpc_log_message(message_id, message)
        logger.info(f"Message {message_id} successfully processed by logging-service.")
        return {"status": "Message sent to logging service", "uuid": message_id, "msg": message}
    except HTTPException as e: # Catch HTTPExceptions raised by resilient_grpc_log_message
        logger.error(f"HTTPException while sending message {message_id} to logging-service: {e.status_code} - {e.detail}")
        raise e # Re-raise the HTTPException to be returned to the client
    except Exception as e: # Catch any other unexpected errors
        logger.error(f"Unexpected error while sending message {message_id} to logging-service: {type(e).__name__} - {str(e)}")
        raise HTTPException(status_code=500, detail=f"Помилка при відправці повідомлення до logging-service: {str(e)}")


@app.get("/message")
async def get_all_messages():
    logger.info("GET /message: Fetching logs from logging-service and messages-service...")
    logged_messages_content = []
    logging_service_error = None

    try:
        grpc_response = await resilient_grpc_get_logs()
        logged_messages_content = list(grpc_response.messages)
        logger.info(f"Retrieved {len(logged_messages_content)} messages from logging-service.")
    except HTTPException as e:
        logger.error(f"HTTPException when fetching logs from logging-service: {e.status_code} - {e.detail}")
        logging_service_error = f"Error fetching logs: {e.detail} (Code: {e.status_code})"
    except Exception as e:
        logger.error(f"Unexpected error when fetching logs from logging-service: {type(e).__name__} - {str(e)}")
        logging_service_error = f"Unexpected error fetching logs: {str(e)}"

    messages_service_output_text = "Error: messages-service not reachable or no instances configured."
    msg_service_addresses = SERVICE_ADDRESSES.get("messages-service", [])
    if not msg_service_addresses: # Try a refresh if empty
        msg_service_addresses = refresh_service_addresses("messages-service")

    if msg_service_addresses:
        # For simplicity, try the first configured messages-service. 
        # Could be extended with failover similar to gRPC if needed.
        target_url_base = msg_service_addresses[0]
        if not target_url_base.startswith("http://") and not target_url_base.startswith("https://"):
            target_url_base = f"http://{target_url_base}" # Default to http
        
        target_messages_service_url = f"{target_url_base}/messages"
        logger.info(f"Fetching from messages-service: {target_messages_service_url}")
        try:
            # Add timeout to requests.get
            r = requests.get(target_messages_service_url, timeout=3.0) 
            r.raise_for_status()
            messages_service_output_text = r.text
            logger.info(f"Response from messages-service: {messages_service_output_text}")
        except requests.RequestException as e:
            logger.error(f"Error connecting to messages-service at {target_messages_service_url}: {e}")
            messages_service_output_text = f"Error connecting to messages-service ({target_messages_service_url}): {e}"
    else:
        logger.warning("No addresses configured or fetched for messages-service.")
        
    response_payload = {
        "logged_messages_from_hazelcast": logged_messages_content,
        "messages_service_output": messages_service_output_text,
    }
    if logging_service_error:
        response_payload["logging_service_error"] = logging_service_error

    return response_payload

if __name__ == "__main__":
    # Initial load for direct run, will also run on app startup
    refresh_service_addresses("logging-service")
    refresh_service_addresses("messages-service")
    uvicorn.run(app, host="0.0.0.0", port=8001)