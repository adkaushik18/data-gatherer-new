FROM python:3.10-slim
WORKDIR /app
COPY . .
RUN pip install -r requirements.txt
ENV PYTHONPATH=/app:/app/..
RUN apt-get update && apt-get install -y curl \
    && rm -rf /var/lib/apt/lists/*
RUN curl -o wait-for-it.sh https://raw.githubusercontent.com/vishnubob/wait-for-it/master/wait-for-it.sh \
    && chmod +x wait-for-it.sh

RUN chmod +x wait-for-it.sh

ENTRYPOINT ["/app/wait-for-it.sh", "kafka:9092", "--timeout=60", "--", "python", "mongo_writer.py"]
