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

# Start the app
CMD ["python", "tiktok_consumer.py"]
