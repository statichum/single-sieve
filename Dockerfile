FROM python:3.12-slim

WORKDIR /app

RUN pip install --no-cache-dir requests pyyaml fastapi uvicorn

COPY single-sieve.py .
COPY listener.py .
COPY config.yml .

RUN chmod +x single-sieve.py

CMD ["uvicorn", "listener:app", "--host", "0.0.0.0", "--port", "8000"]
