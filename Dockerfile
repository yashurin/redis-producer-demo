# Use a lightweight Python image
FROM python:3.11-slim

# Set the working directory
WORKDIR /app

# Copy the producer script
COPY producer.py .

# Install redis-py
RUN pip install redis

# Command to run the producer
CMD ["python", "producer.py"]
