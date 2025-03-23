from fastapi import FastAPI

app = FastAPI()

@app.get("/messages")
async def get_messages():
    return "not implemented yet"
