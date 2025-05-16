import json
import logging
from fastapi import FastAPI, HTTPException
import uvicorn

app = FastAPI()
CONFIG_FILE = "config.json"
SERVICE_CONFIG = {}

# Налаштування логування
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

def load_config():
    global SERVICE_CONFIG
    try:
        with open(CONFIG_FILE, 'r') as f:
            SERVICE_CONFIG = json.load(f)
        logger.info(f"Configuration loaded from {CONFIG_FILE}: {SERVICE_CONFIG}")
    except FileNotFoundError:
        logger.error(f"Configuration file {CONFIG_FILE} not found.")
        SERVICE_CONFIG = {} # Default to empty if file not found
    except json.JSONDecodeError:
        logger.error(f"Error decoding JSON from {CONFIG_FILE}.")
        SERVICE_CONFIG = {}


@app.on_event("startup")
async def startup_event():
    load_config()

@app.get("/services/{service_name}")
async def get_service_addresses(service_name: str):
    if not SERVICE_CONFIG: # Attempt to reload if empty (e.g. initial load failed)
        load_config()

    service_info = SERVICE_CONFIG.get(service_name)
    if not service_info or "addresses" not in service_info:
        logger.warning(f"Service '{service_name}' not found in configuration or addresses missing.")
        raise HTTPException(status_code=404, detail=f"Service '{service_name}' not found or not configured properly.")
    
    logger.info(f"Returning addresses for service '{service_name}': {service_info['addresses']}")
    return {"service_name": service_name, "addresses": service_info["addresses"]}

if __name__ == "__main__":
    # Load config initially for direct run
    load_config()
    uvicorn.run(app, host="0.0.0.0", port=8000)