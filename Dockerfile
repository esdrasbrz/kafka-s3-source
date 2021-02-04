FROM python:3.8

RUN mkdir /app
WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt
RUN pip install confluent_kafka[avro]

COPY sync.py .