FROM python:3.10-slim
WORKDIR /app
COPY . .
RUN pip install -r requirements.txt
ENV PYTHONPATH=/app:/app/..
CMD ["python", "mongo_writer.py"]

