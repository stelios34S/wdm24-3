import uuid

import pika
import json
import os
import time

RABBITMQ_HOST = os.getenv('RABBITMQ_HOST')

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

# def publish_event(queue_name, event_type, data):
#     event = {'type': event_type, 'data': data}
#     channel.basic_publish(
#         exchange='',
#         routing_key=queue_name,
#         body=json.dumps(event)
#     )
#     pika.logging.info(f"Published event to {queue_name}: {event}")
def rpc_call(queue_name, event_type, data, response_queue_name, timeout=30):
    connection = get_rabbitmq_connection()
    channel = connection.channel()
    correlation_id = str(uuid.uuid4())
    response = None
    response_queue = []

    def on_response(ch, method, properties, body):
        if properties.correlation_id == correlation_id:
            response_queue.append(json.loads(body))

    channel.queue_declare(queue=response_queue_name)
    channel.basic_consume(queue=response_queue_name, on_message_callback=on_response, auto_ack=True)

    publish_event(queue_name, event_type, data, reply_to=response_queue_name, correlation_id=correlation_id)

    start_time = time.time()
    while not response_queue:
        if time.time() - start_time > timeout:
            raise TimeoutError("RPC call timed out")
        connection.process_data_events()
    response = response_queue.pop(0)
    connection.close()
    return response

def publish_event(queue_name, event_type, data):
    try:
        event = {'type': event_type, 'data': data}
        channel.basic_publish(
            exchange="",
            routing_key=queue_name,
            body=json.dumps(event),
        )
        print(f"Published event to {queue_name}: {event}")
    except pika.exceptions.AMQPError:
        raise Exception("Failed to publish event")

def start_subscriber(queue_name, callback):
    channel.queue_declare(queue=queue_name)
    channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)
    pika.logging.info(f"Waiting for messages in {queue_name}. To exit press CTRL+C")
    channel.start_consuming()

