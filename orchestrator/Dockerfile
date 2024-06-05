FROM python:3.12-slim

WORKDIR /home/flask-app

COPY ./requirements.txt .

RUN pip install -r requirements.txt

# Copy the rabbitmq_utils.py file
COPY ./rabbitmq_utils.py .

COPY . .

EXPOSE 8001