FROM python:3.10-slim

WORKDIR /opt/app

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

RUN apt-get update \
    && apt-get install -y curl \
    && apt-get clean

COPY requirements.txt requirements.txt
COPY schema/es_schema.sh es_schema.sh

RUN pip install --upgrade pip \
    && pip install -r requirements.txt

COPY . .
