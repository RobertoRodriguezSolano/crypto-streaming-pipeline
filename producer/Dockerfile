FROM mcr.microsoft.com/devcontainers/python:3.11

WORKDIR /app

# Copy requirements and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy code
COPY producer.py .

# Run producer
CMD ["python", "-u", "producer.py"]