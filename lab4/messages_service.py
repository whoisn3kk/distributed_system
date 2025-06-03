from fastapi import FastAPI, HTTPException
import uvicorn
import logging
import argparse
import json
from kafka import KafkaConsumer, TopicPartition
from kafka.errors import KafkaError
import threading
import time

# Global in-memory store for messages of this instance
MESSAGES_STORE = []
SERVICE_PORT = 0 # Global variable to store the service port
KAFKA_TOPIC = "lab4_messages_topic"
KAFKA_BROKERS = ['localhost:9092', 'localhost:9093', 'localhost:9094']

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = FastAPI()

def kafka_consumer_thread():
    global MESSAGES_STORE, SERVICE_PORT
    while True: # Keep trying to connect
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BROKERS,
                auto_offset_reset='earliest', # Start reading at the earliest message if no offset found
                enable_auto_commit=True,
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                # consumer_timeout_ms=1000 # Timeout to allow loop to check for shutdown
            )
            logger.info(f"Kafka consumer connected to brokers: {KAFKA_BROKERS}, topic: {KAFKA_TOPIC}", extra={'service_port': SERVICE_PORT})
            
            for message in consumer:
                # message value and key are raw bytes -- decode if necessary!
                # e.g., for unicode: `message.value.decode('utf-8')`
                log_message = f"Received message: ID={message.key}, Offset={message.offset}, Partition={message.partition}, Value={message.value}"
                logger.info(log_message, extra={'service_port': SERVICE_PORT})
                MESSAGES_STORE.append(message.value) # Assuming message.value is the dict { "uuid": "...", "msg": "..." }
            
            # If consumer_timeout_ms is reached and no new messages, the loop will exit and retry connection.
            # This can be adjusted or handled differently if needed.
            # logger.info("Consumer timed out, will try to reconnect.", extra={'service_port': SERVICE_PORT})


        except KafkaError as e:
            logger.error(f"Kafka consumer error: {e}. Retrying in 5 seconds...", extra={'service_port': SERVICE_PORT})
            time.sleep(5)
        except Exception as e:
            logger.error(f"Unexpected error in Kafka consumer thread: {e}. Retrying in 5 seconds...", extra={'service_port': SERVICE_PORT})
            time.sleep(5)


@app.on_event("startup")
async def startup_event():
    # Start Kafka consumer in a background thread
    thread = threading.Thread(target=kafka_consumer_thread, daemon=True)
    thread.start()
    logger.info("Messages Service started. Kafka consumer thread initiated.", extra={'service_port': SERVICE_PORT})

@app.get("/messages")
async def get_messages_from_instance():
    logger.info(f"Request /messages received, returning {len(MESSAGES_STORE)} messages from this instance.", extra={'service_port': SERVICE_PORT})
    return {"instance_port": SERVICE_PORT, "messages": MESSAGES_STORE}

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Messages Service with Kafka Consumer")
    parser.add_argument('--port', type=int, required=True, help='Port for the HTTP server')
    args = parser.parse_args()
    SERVICE_PORT = args.port
    
    uvicorn.run(app, host="0.0.0.0", port=SERVICE_PORT)