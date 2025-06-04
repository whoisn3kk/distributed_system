FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py .

# Змінна середовища для порту, на якому працюватиме Uvicorn всередині контейнера
# Це значення буде встановлено через docker-compose.yml для кожного secondary
ENV APP_PORT=5001 
EXPOSE $APP_PORT

# Інші змінні середовища
ENV ARTIFICIAL_DELAY="0"
ENV HOSTNAME="secondary"

# Запуск Uvicorn. "--reload" для зручності розробки..
CMD uvicorn app:app --host 0.0.0.0 --port $APP_PORT --reload