FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py .

# Порт, на якому працює FastAPI додаток Майстра (Uvicorn)
EXPOSE 5000

# Змінна середовища для URL вторинних серверів
ENV SECONDARY_URLS=""

# Запуск Uvicorn. "--reload" для зручності розробки, можна прибрати для production.
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "5000", "--reload"]