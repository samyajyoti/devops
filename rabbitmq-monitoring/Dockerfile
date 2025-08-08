FROM python:3.10
ENV PYTHONUNBUFFERED 1
#Running Cron
RUN apt-get update
RUN apt-get update && apt-get install -y docker.io
# App setup
ADD . /code
WORKDIR /code
# Requirements installation
RUN pip install --no-cache-dir -r requirements.txt
