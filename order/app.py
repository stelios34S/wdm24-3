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

from rabbitmq_utils import publish_event, rpc_call, start_subscriber

DB_ERROR_STR = "DB error"
REQ_ERROR_STR = "Requests error"
GATEWAY_URL = "http://orchestrator:8001"

app = Flask("order-service")
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

logging.getLogger("pika").setLevel(logging.WARNING)
logger.info(f'{GATEWAY_URL} gateway url')
db: redis.Redis = redis.Redis(host=os.environ['REDIS_HOST'],
                              port=int(os.environ['REDIS_PORT']),
                              password=os.environ['REDIS_PASSWORD'],
                              db=int(os.environ['REDIS_DB']))


def close_db_connection():
    db.close()


atexit.register(close_db_connection)


class OrderValue(Struct):
    paid: bool
    items: list[tuple[str, int]]
    user_id: str
    total_cost: int


def send_post_request_orch(url, data):
    try:
        response = requests.post(url, json=data)
        response.raise_for_status()
        logger.info(f"Sent POST request to {url} with data {data}")
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to send POST request to {url}: {e}")


def get_order_from_db(order_id: str) -> OrderValue | None: ###Does not need to get transferred
    try:
        # get serialized data
        entry: bytes = db.get(order_id)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    # deserialize data if it exists else return null
    entry: OrderValue | None = msgpack.decode(entry, type=OrderValue) if entry else None
    if entry is None:
        # if order does not exist in the database; abort
        abort(400, f"Order: {order_id} not found!")
    return entry


# @app.post('/create/<user_id>')
def create_order(data): #### ENDPOINT TRANSFERRED TO ORCHESTRATOR
    key = data['order_id']
    user_id = data['user_id']

    value = msgpack.encode(OrderValue(paid=False, items=[], user_id=user_id, total_cost=0))
    try:
        db.set(key, value)
        logger.info(f"Order created: {key}, for user {user_id}")
    except redis.exceptions.RedisError:
        send_post_request_orch(f"{GATEWAY_URL}/acks", json.dumps({'type': 'CreateOrder', 'status': 'failed', 'correlation_id':  key}))
        abort(400, DB_ERROR_STR)
    send_post_request_orch(f"{GATEWAY_URL}/acks", json.dumps({'type': 'CreateOrder', 'status': 'succeeded', 'correlation_id': key}))


@app.post('/batch_init/<n>/<n_items>/<n_users>/<item_price>') ###Does not need to get transferred
def batch_init_users(n: int, n_items: int, n_users: int, item_price: int):
    n = int(n)
    n_items = int(n_items)
    n_users = int(n_users)
    item_price = int(item_price)

    # logger.info(f"Batch init  n: {n}, n_items: {n_items}, n_users: {n_users}, item_price: {item_price}")

    def generate_entry() -> OrderValue:
        user_id = random.randint(0, n_users - 1)
        item1_id = random.randint(0, n_items - 1)
        item2_id = random.randint(0, n_items - 1)
        value = OrderValue(paid=False,
                           items=[(f"{item1_id}", 1), (f"{item2_id}", 1)],
                           user_id=f"{user_id}",
                           total_cost=2 * item_price)
        return value

    kv_pairs: dict[str, bytes] = {f"{i}": msgpack.encode(generate_entry())
                                  for i in range(n)}
    try:
        db.mset(kv_pairs)
        logger.info("Batch init for orders successful")
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({"msg": "Batch init for orders successful"})


@app.get('/find/<order_id>') ###Does not need to get transferred
def find_order(order_id: str):
    order_entry: OrderValue = get_order_from_db(order_id)
    return jsonify(
        {
            "order_id": order_id,
            "paid": order_entry.paid,
            "items": order_entry.items,
            "user_id": order_entry.user_id,
            "total_cost": order_entry.total_cost
        }
    )


def send_post_request(url: str):
    try:
        response = requests.post(url)
    except requests.exceptions.RequestException:
        abort(400, REQ_ERROR_STR)
    else:
        return response


def send_get_request(url: str):
    try:
        response = requests.get(url)
    except requests.exceptions.RequestException:
        abort(400, REQ_ERROR_STR)
    else:
        return response


# @app.post('/addItem/<order_id>/<item_id>/<quantity>')
def add_item(data): #### ENDPOINT TRANSFERRED TO ORCHESTRATOR
    order_id = data['order_id']
    item_id = data['item_id']
    quantity = data['quantity']
    order_entry: OrderValue = get_order_from_db(order_id)
    item_reply =  send_get_request(f"{GATEWAY_URL}/stock/find/{item_id}")
    if item_reply.status_code != 200:
        # Request failed because item does not exist
        send_post_request_orch(f"{GATEWAY_URL}/acks", json.dumps({'type': 'ItemAdded', 'status': 'failed', 'correlation_id': order_id}))
        abort(400, f"Item: {item_id} does not exist!")
    item_json: dict = item_reply.json()
    order_entry.items.append((item_id, int(quantity)))
    order_entry.total_cost += int(quantity) * item_json["price"]
    try:
        db.set(order_id, msgpack.encode(order_entry))
        logger.info(f"Item: {item_id} added to: {order_id} price updated to: {order_entry.total_cost}")
    except redis.exceptions.RedisError:
        send_post_request_orch(f"{GATEWAY_URL}/acks", json.dumps({'type': 'ItemAdded', 'status': 'failed', 'correlation_id': order_id}))
        abort(400, REQ_ERROR_STR)
    send_post_request_orch(f"{GATEWAY_URL}/acks", json.dumps({'type': 'ItemAdded', 'status': 'succeeded','correlation_id': order_id,
                                                              'data': {'order_id': order_id,
                                                                       'total_cost': order_entry.total_cost}}))


#####STILL UNSURE ABOUT THIS METHOF
def rollback_stock(removed_items: list[tuple[str, int]]):
    for item_id, quantity in removed_items:
        send_post_request(f"{GATEWAY_URL}/stock/add/{item_id}/{quantity}")


# @app.post('/checkout/<order_id>')
def checkout(data): #### ENDPOINT TRANSFERRED TO ORCHESTRATOR
    order_id = data['order_id']
    try:
        order_entry: OrderValue = get_order_from_db(order_id)
        publish_event("events_payment", "ProcessPayment", {
            'order_id': order_id,
            'user_id': order_entry.user_id,
            'total_cost': order_entry.total_cost,
            'items': order_entry.items,
        })
    except redis.exceptions.RedisError:
        send_post_request_orch(f"{GATEWAY_URL}/acks", json.dumps({'type': 'Checkout', 'status': 'failed', 'correlation_id': order_id}))
        abort(400, DB_ERROR_STR)
    #send_post_request_orch(f"{GATEWAY_URL}/acks", json.dumps({'type': 'Checkout', 'status': 'initiated'}))


def handle_issue_refund(data):
    order_id = data['order_id']
    send_post_request_orch(f"{GATEWAY_URL}/acks", json.dumps({'type': 'IssueRefund', 'status': 'succeed', 'correlation_id': order_id}))

def handle_payment_successful(data):
    order_id = data['order_id']
    logger.info(f"Received payment, data: {data}")
    order_entry: OrderValue = get_order_from_db(order_id)
    order_entry.paid = True
    try:
        db.set(order_id, msgpack.encode(order_entry))
        logger.info(f"Payment successful for order {order_id}")
    except redis.exceptions.RedisError:
        abort(400, DB_ERROR_STR)


def handle_payment_failed(data):
    order_id = data['order_id']
    order_entry: OrderValue = get_order_from_db(order_id)
    order_entry.paid = False
    try:
        db.set(order_id, msgpack.encode(order_entry))
        logger.info(f"Payment failed for order {order_id}")
        send_post_request_orch(f"{GATEWAY_URL}/acks", json.dumps({'type': 'Checkout', 'status': 'failed', 'correlation_id': order_id}))
    except redis.exceptions.RedisError:
        abort(400, DB_ERROR_STR)


def handle_stock_reserved(data):
    order_id = data['order_id']
    logger.debug(f"Checkout successful for order {order_id}")
    send_post_request_orch(f"{GATEWAY_URL}/acks", json.dumps({'type': 'Checkout', 'status': 'succeed', 'correlation_id': order_id}))



def handle_stock_failed(data):
    order_id = data['order_id']
    order_entry: OrderValue = get_order_from_db(order_id)
    publish_event('events_payment', 'IssueRefund', {
        'order_id': order_id,
        'user_id': order_entry.user_id,
        'total_cost': order_entry.total_cost
    })
    #send_post_request_orch(f"{GATEWAY_URL}/acks", json.dumps({'type': 'Stock', 'status': 'failed'})) ###???? maybe we dont need this
    logger.info(f"Stock failed for order: {order_id}")



def process_event(ch, method, properties, body):
    event = json.loads(body)
    event_type = event['type']
    data = json.loads(event['data'])
    if event_type == 'OrderCreation':
        create_order(data)
    if event_type == 'AddItem':
        add_item(data)
    if event_type == 'Checkout':
        checkout(data)
    if event_type == 'RefundIssued':
        handle_issue_refund(data)
    if event_type == 'PaymentSuccessfulOrder':
        handle_payment_successful(data)
    elif event_type == 'PaymentFailed':
        handle_payment_failed(data)
    elif event_type == 'StockReserved':
        handle_stock_reserved(data)
    elif event_type == 'StockFailed':
        handle_stock_failed(data)



start_subscriber('events_order', process_event)

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
