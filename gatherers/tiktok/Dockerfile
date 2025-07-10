FROM python:3.10-slim

# Set working directory
WORKDIR /app

# Copy only this gatherer’s context (Docker build context should include whole repo)
COPY . .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Show logs immediately
ENV PYTHONUNBUFFERED=1

# Add the parent of 'gatherers' to PYTHONPATH (assuming it's one level up from current)
ENV PYTHONPATH=/app:/app/..
RUN apt-get update && apt-get install -y curl \
    && rm -rf /var/lib/apt/lists/*
RUN curl -o wait-for-it.sh https://raw.githubusercontent.com/vishnubob/wait-for-it/master/wait-for-it.sh \
    && chmod +x wait-for-it.sh

RUN chmod +x wait-for-it.sh

# Default command (optional)
ENTRYPOINT ["/app/wait-for-it.sh", "kafka:9092", "--timeout=60", "--", "python", "tiktok_consumer.py"]


