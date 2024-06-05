import logging
import os
import atexit
import json
from threading import Thread
import uuid

import requests

from event import Event
from queue import Queue
import redis

from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response
from rabbitmq_utils import publish_event, start_subscriber

DB_ERROR_STR = "DB error"

app = Flask("stock-service")
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
GATEWAY_URL = os.environ["GATEWAY_URL"]
logging.getLogger("pika").setLevel(logging.WARNING)

db: redis.Redis = redis.Redis(host=os.environ['REDIS_HOST'],
                              port=int(os.environ['REDIS_PORT']),
                              password=os.environ['REDIS_PASSWORD'],
                              db=int(os.environ['REDIS_DB']))


def close_db_connection():
    db.close()


atexit.register(close_db_connection)


def send_post_request_orch(url, data):
    try:
        response = requests.post(url, json=data)
        response.raise_for_status()
        logger.info(f"Sent POST request to {url} with data {data}")
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to send POST request to {url}: {e}")


class StockValue(Struct):
    stock: int
    price: int


def get_item_from_db(item_id: str) -> StockValue | None:
    # get serialized data
    try:
        entry: bytes = db.get(item_id)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    # deserialize data if it exists else return null
    entry: StockValue | None = msgpack.decode(entry, type=StockValue) if entry else None
    if entry is None:
        # if item does not exist in the database; abort
        abort(400, f"Item: {item_id} not found!")
    return entry


# @app.post('/item/create/<price>')
def create_item(data): ####Transfered to orchestrator
    price = data["price"]
    key = data["item_id"]
    logger.debug(f"Item: {key} created")
    value = msgpack.encode(StockValue(stock=0, price=int(price)))
    try:
        db.set(key, value)
        publish_event('events_orchestrator', 'CreateItem', {'correlation_id': key, 'status': 'succeed'})
        return jsonify({'item_id': key})
    except redis.exceptions.RedisError:
        publish_event('events_orchestrator', 'CreateItem', {'correlation_id': key, 'status': 'failed'})
        abort(400, DB_ERROR_STR)



@app.post('/batch_init/<n>/<starting_stock>/<item_price>')
def batch_init_users(n: int, starting_stock: int, item_price: int):
    kv_pairs: dict[str, bytes] = {f"{i}": msgpack.encode(StockValue(stock=starting_stock, price=item_price))
                                  for i in range(n)}
    try:
        db.mset(kv_pairs)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({"msg": "Batch init for stock successful"})


#@app.get('/find/<item_id>')
def find_item(data):
    order_id = data['order_id']
    item_id = data['item_id']
    quantity = data['quantity']
    try:
        item_entry: StockValue = get_item_from_db(item_id)
        publish_event("events_order", "AddItem", {"item_id": item_id, "order_id": order_id, "quantity": quantity, "price": item_entry.price,"status": "succeed"})
    except redis.exceptions.RedisError:
        publish_event("events_order", "AddItem", {"item_id": item_id, "order_id": order_id, "quantity": quantity ,"status": "failed"})
        abort(400, DB_ERROR_STR)


#@app.post('/add/<item_id>/<amount>') ####Transfered to orchestrator
def add_stock(data):
    item_id = data["item_id"]
    amount = data["amount"]
    logger.info(f"Adding {amount} for {item_id}")
    item_entry: StockValue = get_item_from_db(item_id)
    # update stock, serialize and update database
    item_entry.stock += int(amount)
    try:
        db.set(item_id, msgpack.encode(item_entry))
        publish_event('events_orchestrator', 'AddStock', {'correlation_id': item_id, 'status': 'succeed'})
        return jsonify({"msg": f"Item: {item_id} stock updated to: {item_entry.stock}"})
    except redis.exceptions.RedisError:
        publish_event('events_orchestrator', 'AddStock', {'correlation_id': item_id, 'status': 'failed'})
        abort(400, DB_ERROR_STR)



#@app.post('/subtract/<item_id>/<amount>') ###transfer to orchestrator
def remove_stock(data):
    item_id = data["item_id"]
    amount = data["amount"]
    item_entry: StockValue = get_item_from_db(item_id)
    # update stock, serialize and update database
    item_entry.stock -= int(amount)
    logger.debug(f"Item: {item_id} stock updated to: {item_entry.stock}")
    if item_entry.stock < 0:
        publish_event('events_orchestrator', 'RemoveStock', {'correlation_id': item_id, 'status': 'failed'})
        abort(400, f"Item: {item_id} stock cannot get reduced below zero!")
    try:
        db.set(item_id, msgpack.encode(item_entry))
        publish_event('events_orchestrator', 'RemoveStock', {'correlation_id': item_id, 'status': 'succeed'})
    except redis.exceptions.RedisError:
        publish_event('events_orchestrator', 'RemoveStock', {'correlation_id': item_id, 'status': 'failed'})
        abort(400, DB_ERROR_STR)
    #return Response(f"Item: {item_id} stock updated to: {item_entry.stock}", status=200)


def handle_payment_successful(data):
    order_id = data['order_id']
    items = data['items']
    stock_check = True
    for item_id, quantity in items:
        item_entry: StockValue = get_item_from_db(item_id)
        if item_entry.stock < quantity:
            stock_check = False
            break
    if stock_check:
        for item_id, quantity in items:
            item_entry: StockValue = get_item_from_db(item_id)
            item_entry.stock -= quantity
            try:
                db.set(item_id, msgpack.encode(item_entry))
            except redis.exceptions.RedisError:
                return abort(400, DB_ERROR_STR)
        publish_event('events_order', 'StockReserved', {
            'order_id': order_id,
        })
        #send_post_request_orch(f"{GATEWAY_URL}/acks", json.dumps({'type': 'Checkout', 'status': 'succeed'}))
    else:
        #send_post_request_orch(f"{GATEWAY_URL}/acks", json.dumps({'type': 'PaymentSuccessful', 'status': 'failed'}))
        publish_event('events_order', 'StockFailed', {
            'order_id': order_id,
        })



def process_event(ch, method, properties, body):
    event = json.loads(body)
    event_type = event['type']
    data = event['data']
    if event_type == 'PaymentSuccessfulStock':
        handle_payment_successful(data)
    if event_type == 'CreateItem':
        create_item(data)
    if event_type == 'AddStock':
        add_stock(data)
    if event_type == 'RemoveStock':
        remove_stock(data)
    if event_type == 'AddItemCheck':
        find_item(data)

start_subscriber('events_stock', process_event)

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
