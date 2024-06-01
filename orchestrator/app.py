import logging
import os
import atexit
import random
import json
from threading import Thread
import uuid
from queue import Queue

import redis
import requests

from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response
from rabbitmq_utils import publish_event, start_subscriber


DB_ERROR_STR = "DB error"
REQ_ERROR_STR = "Requests error"

GATEWAY_URL = os.environ['GATEWAY_URL']

app = Flask("orchestrator")
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

logging.getLogger("pika").setLevel(logging.WARNING)



@app.post('/create/<user_id>')
def create_order(user_id: str):
    try:
        publish_event("events","OrderCreation",json.dumps(user_id))
        #logger.info(f"Order created: {key}, for user {user_id}")
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response("Order Created",status=200)


@app.post('/batch_init/<n>/<n_items>/<n_users>/<item_price>')
def batch_init_users(n: int, n_items: int, n_users: int, item_price: int):
    try:
        data = {"n": int(n), "n_items": int(n_items), "n_users": int(n_users), "item_price": int(item_price)}
        publish_event("events","BatchInit",json.dumps(data))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({"msg": "Batch init for orders successful"})

#start_subscriber('events', process_event)
def process_event(ch, method, properties, body):
    event = json.loads(body)
    event_type = event['type']
    data = event['data']
    if event_type == 'OrderCreationSuccess':
        logger.info(f"Order created: {data}")
    elif event_type == 'OrderCreationFailed':
        logger.error(f"Order creation failed: {data}")


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
