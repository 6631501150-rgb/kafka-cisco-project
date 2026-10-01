FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY producer_api.py worker.py backend.py ./
COPY frontend ./frontend

CMD ["uvicorn", "producer_api:app", "--host", "0.0.0.0", "--port", "8000"]
