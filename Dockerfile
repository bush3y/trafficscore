FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y libpq-dev gcc && rm -rf /var/lib/apt/lists/*

COPY requirements-api.txt .
RUN pip install --no-cache-dir -r requirements-api.txt

COPY api/ ./api/
COPY frontend/ ./frontend/

CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000"]
