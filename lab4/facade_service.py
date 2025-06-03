import logging
from fastapi import FastAPI, HTTPException
import uuid
import grpc
import requests
from tenacity import retry, wait_fixed, stop_after_attempt, before_sleep_log, retry_if_exception_type
import random
import json # For Kafka messages
from kafka import KafkaProducer
from kafka.errors import KafkaError
import time

import logging_pb2
import logging_pb2_grpc
import uvicorn

app = FastAPI()
SERVICE_PORT = 8001 # Default port for Facade

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Global cache for service addresses
SERVICE_ADDRESSES = {}
CONFIG_SERVER_URL = "http://localhost:8000/services"
KAFKA_BROKERS = ['localhost:9092', 'localhost:9093', 'localhost:9094']
KAFKA_TOPIC = "lab4_messages_topic"
kafka_producer = None

def initialize_kafka_producer():
    global kafka_producer
    while kafka_producer is None:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                retries=5 # Retry sending a message up to 5 times
            )
            kafka_producer = producer
            logger.info(f"Kafka producer initialized and connected to brokers: {KAFKA_BROKERS}", extra={'service_port': SERVICE_PORT})
        except KafkaError as e:
            logger.error(f"Failed to initialize Kafka producer: {e}. Retrying in 5 seconds...", extra={'service_port': SERVICE_PORT})
            time.sleep(5)
        except Exception as e: # Catch any other error
            logger.error(f"Unexpected error initializing Kafka producer: {e}. Retrying in 5 seconds...", extra={'service_port': SERVICE_PORT})
            time.sleep(5)


def refresh_service_addresses(service_name: str) -> list:
    global SERVICE_ADDRESSES
    try:
        url = f"{CONFIG_SERVER_URL}/{service_name}"
        logger.info(f"Fetching addresses for '{service_name}' from {url}", extra={'service_port': SERVICE_PORT})
        response = requests.get(url, timeout=3)
        response.raise_for_status()
        data = response.json()
        addresses = data.get("addresses", [])
        SERVICE_ADDRESSES[service_name] = addresses
        logger.info(f"Refreshed addresses for {service_name}: {addresses}", extra={'service_port': SERVICE_PORT})
        if not addresses:
             logger.warning(f"No addresses returned from config server for '{service_name}'.", extra={'service_port': SERVICE_PORT})
        return addresses
    except requests.RequestException as e:
        logger.error(f"Failed to fetch addresses for {service_name} from config server: {e}. Using cached: {SERVICE_ADDRESSES.get(service_name, [])}", extra={'service_port': SERVICE_PORT})
        return SERVICE_ADDRESSES.get(service_name, [])
    except Exception as e:
        logger.error(f"Unexpected error fetching addresses for {service_name}: {e}. Using cached: {SERVICE_ADDRESSES.get(service_name, [])}", extra={'service_port': SERVICE_PORT})
        return SERVICE_ADDRESSES.get(service_name, [])

@app.on_event("startup")
async def startup_event():
    logger.info(f"Facade Service starting up on port {SERVICE_PORT}. Initializing service addresses and Kafka producer...", extra={'service_port': SERVICE_PORT})
    refresh_service_addresses("logging-service")
    refresh_service_addresses("messages-service")
    initialize_kafka_producer() # Initialize Kafka producer

@app.on_event("shutdown")
async def shutdown_event():
    if kafka_producer:
        kafka_producer.flush() # Ensure all pending messages are sent
        kafka_producer.close()
        logger.info("Kafka producer closed.", extra={'service_port': SERVICE_PORT})


def execute_grpc_call_with_failover(service_name: str, call_func):
    # (This function remains largely the same as your provided one)
    current_addresses = SERVICE_ADDRESSES.get(service_name, [])
    
    if not current_addresses:
        logger.warning(f"No cached addresses for {service_name}, attempting refresh from config server.", extra={'service_port': SERVICE_PORT})
        current_addresses = refresh_service_addresses(service_name)

    if not current_addresses:
        logger.error(f"Still no addresses available for {service_name} after refresh.", extra={'service_port': SERVICE_PORT})
        raise HTTPException(status_code=503, detail=f"No instances available for service '{service_name}'.")

    shuffled_addresses = list(current_addresses)
    random.shuffle(shuffled_addresses)
    last_exception = None

    for address in shuffled_addresses:
        channel = None
        try:
            logger.info(f"Attempting gRPC call to {service_name} at {address}", extra={'service_port': SERVICE_PORT})
            channel = grpc.insecure_channel(address)
            grpc.channel_ready_future(channel).result(timeout=1.0) 
            stub = logging_pb2_grpc.LoggingServiceStub(channel)
            response = call_func(stub)
            logger.info(f"Successfully connected and called {service_name} at {address}", extra={'service_port': SERVICE_PORT})
            return response
        except grpc.FutureTimeoutError:
            logger.warning(f"Timeout connecting to {service_name} instance at {address}.", extra={'service_port': SERVICE_PORT})
            last_exception = HTTPException(status_code=504, detail=f"Timeout connecting to {service_name} instance at {address}")
        except grpc.RpcError as e:
            logger.warning(f"gRPC call to {service_name} instance at {address} failed: {e.code()} - {e.details()}", extra={'service_port': SERVICE_PORT})
            if e.code() == grpc.StatusCode.UNAVAILABLE:
                last_exception = HTTPException(status_code=503, detail=f"Service {service_name} at {address} unavailable: {e.details()}")
            else:
                last_exception = HTTPException(status_code=500, detail=f"gRPC error with {service_name} at {address}: {e.details()}")
        except Exception as e:
            logger.error(f"Unexpected error with {service_name} at {address}: {type(e).__name__} - {e}", extra={'service_port': SERVICE_PORT})
            last_exception = HTTPException(status_code=500, detail=f"Unexpected error with {service_name} at {address}: {str(e)}")
        finally:
            if channel:
                channel.close()
    
    logger.error(f"All instances of {service_name} failed. Last error: {last_exception.detail if last_exception else 'Unknown'}. Triggering address refresh.", extra={'service_port': SERVICE_PORT})
    refresh_service_addresses(service_name)
    if last_exception:
        raise last_exception
    else:
        raise HTTPException(status_code=503, detail=f"All instances of {service_name} failed, no specific error captured.")

@retry(
    wait=wait_fixed(1), stop=stop_after_attempt(3),
    before_sleep=before_sleep_log(logging.getLogger(), logging.WARNING),
    retry=retry_if_exception_type((HTTPException, grpc.RpcError))
)
async def resilient_grpc_log_message(message_id: str, msg: str):
    def call_logic(stub):
        request = logging_pb2.LogRequest(uuid=message_id, msg=msg)
        return stub.Log(request, timeout=2.0) 
    return execute_grpc_call_with_failover("logging-service", call_logic)

@retry(
    wait=wait_fixed(1), stop=stop_after_attempt(3),
    before_sleep=before_sleep_log(logging.getLogger(), logging.WARNING),
    retry=retry_if_exception_type((HTTPException, grpc.RpcError))
)
async def resilient_grpc_get_logs():
    def call_logic(stub):
        empty = logging_pb2.Empty()
        return stub.GetLogs(empty, timeout=5.0)
    return execute_grpc_call_with_failover("logging-service", call_logic)


@app.post("/message")
async def post_message(item: dict):
    global kafka_producer
    if "msg" not in item:
        raise HTTPException(status_code=400, detail="Missing 'msg' field")
    message_content = item["msg"]
    message_id = str(uuid.uuid4())
    
    kafka_payload = {"uuid": message_id, "msg": message_content}

    try:
        # 1. Send to logging-service via gRPC
        logger.info(f"POST /message: UUID={message_id}, Msg='{message_content}'. Sending to logging-service...", extra={'service_port': SERVICE_PORT})
        await resilient_grpc_log_message(message_id, message_content)
        logger.info(f"Message {message_id} successfully processed by logging-service.", extra={'service_port': SERVICE_PORT})

        # 2. Send to Kafka for messages-service
        if not kafka_producer: # Should have been initialized on startup
            logger.error("Kafka producer is not available. Message cannot be sent to Kafka.", extra={'service_port': SERVICE_PORT})
            # Optionally, re-attempt initialization or raise a more specific error
            initialize_kafka_producer() # Try to re-initialize
            if not kafka_producer:
                 raise HTTPException(status_code=503, detail="Kafka producer not available. Message logged but not queued.")

        logger.info(f"Sending message {message_id} to Kafka topic '{KAFKA_TOPIC}'...", extra={'service_port': SERVICE_PORT})
        future = kafka_producer.send(KAFKA_TOPIC, value=kafka_payload, key=message_id.encode('utf-8'))
        # Block for 'synchronous' sends, or handle success/failure asynchronously
        future.get(timeout=10) # Wait for up to 10 seconds for acknowledgement
        logger.info(f"Message {message_id} successfully sent to Kafka topic '{KAFKA_TOPIC}'.", extra={'service_port': SERVICE_PORT})
        
        return {"status": "Message sent to logging service and Kafka", "uuid": message_id, "msg": message_content}

    except KafkaError as e:
        logger.error(f"KafkaError while sending message {message_id} to Kafka: {e}", extra={'service_port': SERVICE_PORT})
        # Message was logged to logging-service, but failed for Kafka
        raise HTTPException(status_code=500, detail=f"Error sending message to Kafka: {str(e)}. Message was logged.")
    except HTTPException as e: # Catch HTTPExceptions from resilient_grpc_log_message
        logger.error(f"HTTPException while processing message {message_id}: {e.status_code} - {e.detail}", extra={'service_port': SERVICE_PORT})
        raise e
    except Exception as e:
        logger.error(f"Unexpected error while processing message {message_id}: {type(e).__name__} - {str(e)}", extra={'service_port': SERVICE_PORT})
        raise HTTPException(status_code=500, detail=f"Error processing message: {str(e)}")


@app.get("/message")
async def get_all_messages_combined():
    logger.info("GET /message: Fetching logs from logging-service and one messages-service instance...", extra={'service_port': SERVICE_PORT})
    logged_messages_content = []
    logging_service_error = None
    messages_service_data = None
    messages_service_error = None

    # 1. Fetch from logging-service (Hazelcast)
    try:
        grpc_response = await resilient_grpc_get_logs()
        # Convert gRPC LogMessage objects to simple strings or dicts if needed. Assuming it's a list of strings.
        logged_messages_content = list(grpc_response.messages) 
        logger.info(f"Retrieved {len(logged_messages_content)} messages from logging-service.", extra={'service_port': SERVICE_PORT})
    except HTTPException as e:
        logger.error(f"HTTPException when fetching logs from logging-service: {e.status_code} - {e.detail}", extra={'service_port': SERVICE_PORT})
        logging_service_error = f"Error fetching logs from logging-service: {e.detail} (Code: {e.status_code})"
    except Exception as e:
        logger.error(f"Unexpected error when fetching logs from logging-service: {type(e).__name__} - {str(e)}", extra={'service_port': SERVICE_PORT})
        logging_service_error = f"Unexpected error fetching logs from logging-service: {str(e)}"

    # 2. Fetch from a random messages-service instance
    msg_service_addresses = SERVICE_ADDRESSES.get("messages-service", [])
    if not msg_service_addresses:
        msg_service_addresses = refresh_service_addresses("messages-service")

    if msg_service_addresses:
        selected_msg_service_address = random.choice(msg_service_addresses)
        target_url = selected_msg_service_address
        if not target_url.startswith("http://") and not target_url.startswith("https://"):
            target_url = f"http://{target_url}" # Default to http
        
        target_messages_service_url = f"{target_url}/messages"
        logger.info(f"Fetching from randomly selected messages-service: {target_messages_service_url}", extra={'service_port': SERVICE_PORT})
        try:
            r = requests.get(target_messages_service_url, timeout=3.0)
            r.raise_for_status()
            messages_service_data = r.json() # Expecting {"instance_port": port, "messages": [...]}
            logger.info(f"Response from messages-service ({target_messages_service_url}): {messages_service_data}", extra={'service_port': SERVICE_PORT})
        except requests.RequestException as e:
            logger.error(f"Error connecting to messages-service at {target_messages_service_url}: {e}", extra={'service_port': SERVICE_PORT})
            messages_service_error = f"Error connecting to messages-service ({target_messages_service_url}): {e}"
        except json.JSONDecodeError as e:
            logger.error(f"Error decoding JSON from messages-service at {target_messages_service_url}: {e}", extra={'service_port': SERVICE_PORT})
            messages_service_error = f"Error decoding JSON from messages-service ({target_messages_service_url}): {e}"
    else:
        logger.warning("No addresses configured or fetched for messages-service.", extra={'service_port': SERVICE_PORT})
        messages_service_error = "No messages-service instances available/configured."
        
    response_payload = {
        "logged_messages_from_hazelcast": logged_messages_content,
        "messages_from_selected_messages_service": messages_service_data,
    }
    if logging_service_error:
        response_payload["logging_service_error"] = logging_service_error
    if messages_service_error:
        response_payload["messages_service_error"] = messages_service_error

    return response_payload

if __name__ == "__main__":
    # SERVICE_PORT is defined as a global at the top for Facade (e.g. 8001)
    # Startup event handles initialization
    uvicorn.run(app, host="0.0.0.0", port=SERVICE_PORT)