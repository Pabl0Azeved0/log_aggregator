FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY src ./src
COPY scripts ./scripts
ENV PYTHONPATH=/app/src
CMD ["uvicorn", "log_aggregator.api.ingest:app", "--host", "0.0.0.0", "--port", "8000"]
