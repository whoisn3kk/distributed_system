from fastapi import FastAPI
import uvicorn
import logging

app = FastAPI()

# Налаштування логування
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


@app.get("/messages")
async def get_messages():
    logger.info("Запит /messages отримано, повертається 'not implemented yet'")
    return "not implemented yet"

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8002)