import pika
import json
import os
import time
from flask import Flask, jsonify, abort, Response
RABBITMQ_HOST = os.getenv('RABBITMQ_HOST')
DB_ERROR_STR = "DB error"
REQ_ERROR_STR = "Requests error"
def get_rabbitmq_connection():
    connection = None
    for i in range(10):  # Retry 10 times
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
            break
        except pika.exceptions.AMQPConnectionError:
            print(f"Retrying to connect to RabbitMQ ({i+1}/10)...")
            time.sleep(5)
    if not connection:
        raise Exception("Failed to connect to RabbitMQ after multiple attempts")
    return connection

connection = get_rabbitmq_connection()
channel = connection.channel()

def publish_event(queue_name, event_type, data):
    try:
        event = {'type': event_type, 'data': data}
        channel.basic_publish(
            exchange='',
            routing_key=queue_name,
            body=json.dumps(event)
        )
        pika.logging.info(f"Published event to {queue_name}: {event}")
    except pika.exceptions.AMQPError:
        return abort(400, REQ_ERROR_STR)
    return jsonify(event)


def start_subscriber(queue_name, callback):
    channel.queue_declare(queue=queue_name)
    channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)
    pika.logging.info(f"Waiting for messages in {queue_name}. To exit press CTRL+C")
    channel.start_consuming()
