FROM python:3.10-slim

WORKDIR /app

# Install system dependencies for compiling wheels (if needed) and Java for Flink
RUN apt-get update && apt-get install -y \
    default-jdk-headless \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java

COPY requirements.txt .

# Uncomment flink for docker build
RUN sed -i 's/# apache-flink/apache-flink/g' requirements.txt

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "producers/cta_producer.py"]
