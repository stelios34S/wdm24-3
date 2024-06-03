import logging
import os
import atexit
import random
import json
from threading import Thread
import uuid
from queue import Queue
from collections import defaultdict

from redis.cluster import ClusterNode, RedisCluster
import requests

from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response
from event import Event
# from rabbitmq_utils import publish_event, start_subscriber


DB_ERROR_STR = "DB error"
REQ_ERROR_STR = "Requests error"

GATEWAY_URL = os.environ['GATEWAY_URL']

app = Flask("order-service")
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# logging.getLogger("pika").setLevel(logging.WARNING)

# db: redis.Redis = redis.Redis(host=os.environ['REDIS_HOST'],
#                               port=int(os.environ['REDIS_PORT']),
#                               password=os.environ['REDIS_PASSWORD'],
#                               db=int(os.environ['REDIS_DB']))

host = os.environ['REDIS_NODES'].split()
nodes = [ClusterNode(host=h, port=os.environ['REDIS_CLUSTER_ANNOUNCE_PORT']) for h in host]
db = RedisCluster(startup_nodes=nodes)

def close_db_connection():
    db.close()


atexit.register(close_db_connection)

class OrderValue(Struct):
    paid: bool
    items: list[tuple[str, int]]
    user_id: str
    total_cost: int


def get_order_from_db(order_id: str) -> OrderValue | None:
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


@app.post('/create/<user_id>')
def create_order(user_id: str):
    key = str(uuid.uuid4())
    value = msgpack.encode(OrderValue(paid=False, items=[], user_id=user_id, total_cost=0))
    try:
        db.set(key, value)
        logger.info(f"Order created: {key}, for user {user_id}")
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({'order_id': key})


@app.post('/batch_init/<n>/<n_items>/<n_users>/<item_price>')
def batch_init_users(n: int, n_items: int, n_users: int, item_price: int):

    n = int(n)
    n_items = int(n_items)
    n_users = int(n_users)
    item_price = int(item_price)

    def generate_entry() -> OrderValue:
        user_id = random.randint(0, n_users - 1)
        item1_id = random.randint(0, n_items - 1)
        item2_id = random.randint(0, n_items - 1)
        value = OrderValue(paid=False,
                           items=[(f"{item1_id}", 1), (f"{item2_id}", 1)],
                           user_id=f"{user_id}",
                           total_cost=2*item_price)
        return value

    kv_pairs: dict[str, bytes] = {f"{i}": msgpack.encode(generate_entry())
                                  for i in range(n)}
    try:
        # db.mset(kv_pairs)
        for k,v in kv_pairs.items():
            db.set(k, v)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({"msg": "Batch init for orders successful"})


@app.get('/find/<order_id>')
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


@app.post('/addItem/<order_id>/<item_id>/<quantity>')
def add_item(order_id: str, item_id: str, quantity: int):
    order_entry: OrderValue = get_order_from_db(order_id)
    item_reply = send_get_request(f"{GATEWAY_URL}/stock/find/{item_id}")
    if item_reply.status_code != 200:
        # Request failed because item does not exist
        abort(400, f"Item: {item_id} does not exist!")
    item_json: dict = item_reply.json()
    order_entry.items.append((item_id, int(quantity)))
    order_entry.total_cost += int(quantity) * item_json["price"]
    try:
        db.set(order_id, msgpack.encode(order_entry))
        logger.info(f"Item: {item_id} added to: {order_id} price updated to: {order_entry.total_cost}")
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"Item: {item_id} added to: {order_id} price updated to: {order_entry.total_cost}",
                    status=200)


def rollback_stock(removed_items: list[tuple[str, int]]):
    for item_id, quantity in removed_items:
        send_post_request(f"{GATEWAY_URL}/stock/add/{item_id}/{quantity}")


@app.post('/checkout/<order_id>')
def checkout(order_id: str):
    app.logger.debug(f"Checking out {order_id}")
    order_entry: OrderValue = get_order_from_db(order_id)
    publish_event("OrderPrepared", {"order_id": order_id, "items":order_entry.items})
    # get the quantity per item
    items_quantities: dict[str, int] = defaultdict(int)
    for item_id, quantity in order_entry.items:
        items_quantities[item_id] += quantity
    # The removed items will contain the items that we already have successfully subtracted stock from
    # for rollback purposes.
    removed_items: list[tuple[str, int]] = []
    for item_id, quantity in items_quantities.items():
        stock_reply = send_post_request(f"{GATEWAY_URL}/stock/subtract/{item_id}/{quantity}")
        if stock_reply.status_code != 200:
            # If one item does not have enough stock we need to rollback
            rollback_stock(removed_items)
            abort(400, f'Out of stock on item_id: {item_id}')
        removed_items.append((item_id, quantity))
    user_reply = send_post_request(f"{GATEWAY_URL}/payment/pay/{order_entry.user_id}/{order_entry.total_cost}")
    if user_reply.status_code != 200:
        # If the user does not have enough credit we need to rollback all the item stock subtractions
        rollback_stock(removed_items)
        abort(400, "User out of credit")
    order_entry.paid = True
    try:
        db.set(order_id, msgpack.encode(order_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    app.logger.debug("Checkout successful")
    return Response("Checkout successful", status=200)

# def checkout(order_id: str):
#     logger.debug(f"Checking out {order_id}")
#     order_entry: OrderValue = get_order_from_db(order_id)
#     publish_event('order_events', 'ProcessPayment', {
#         'order_id': order_id,
#         'user_id': order_entry.user_id,
#         'total_cost': order_entry.total_cost,
#         'items': order_entry.items,
#     })
#     logger.info("Checkout initiated")
#     return Response("Checkout initiated", status=200)

def publish_event(event_type, data):
    event = Event(event_type, data)
    logger.info(f"Publishing event: {event.to_json()}")
    db.publish('order_events', event.to_json())

def handle_event(event):
    data = event.data
    event_type = event.event_type
    if event_type == "PaymentCompleted":
        handle_payment_completed(data)
    elif event_type == "PaymentFailed":
        handle_payment_failed(data)
    elif event_type == "StockReserved":
        handle_stock_reserved(data)
    elif event_type == "StockFailed":
        handle_stock_failed(data)
    
def handle_payment_completed(data):
    order_id = data["order_id"]
    order_entry = get_order_from_db(order_id)
    if order_entry:
        order_entry.paid = True
        try:
            db.set(order_id, msgpack.encode(order_entry))
        except redis.exceptions.RedisError:
            logger.error(f"Failed to update order {order_id} status to paid")
        else:
            logger.info(f"Order {order_id} marked as paid")
        publish_event("OrderCompleted", {"order_id": order_id})

def handle_payment_failed(data):
    order_id = data["order_id"]
    order_entry = get_order_from_db(order_id)
    if order_entry:
        logger.info(f"Order {order_id} payment failed")
    publish_event("OrderFailed", {"order_id": order_id, "items":order_entry.items})

def handle_stock_reserved(data):
    order_id = data["order_id"]
    publish_event("OrderPayment", {"order_id": order_id, "user_id": data["user_id"], "amount": data["total_cost"]})

def handle_stock_failed(data):
    order_id = data["order_id"]
    order_entry = get_order_from_db(order_id)
    if order_entry:
        logger.info(f"Order {order_id} stock reservation failed")
    publish_event("OrderFailed", {"order_id": order_id, "items":order_entry.items})

def subscribe_to_events():
    pubsub = db.pubsub()
    pubsub.subscribe(['payment_events', 'stock_events'])
    logger.info("Subscribed to payment_events and stock_events channels.")
    for message in pubsub.listen():
        if message['type'] == 'message':
            event = Event.from_json(message['data'])
            handle_event(event)

subscriber_thread = Thread(target=subscribe_to_events)
subscriber_thread.start()

# def process_event(ch, method, properties, body):
#     event = json.loads(body)
#     event_type = event['type']
#     data = event['data']
#     if event_type == 'PaymentSuccessful':
#         handle_payment_successful(data)
#     elif event_type == 'PaymentFailed':
#         handle_payment_failed(data)
#     elif event_type == 'StockReserved':
#         handle_stock_reserved(data)
#     elif event_type == 'StockFailed':
#         handle_stock_failed(data)


# def handle_payment_successful(data):
#     order_id = data['order_id']
#     logger.info(f"Received payment, data: {data}")
#     order_entry: OrderValue = get_order_from_db(order_id)
#     order_entry.paid = True
#     try:
#         db.set(order_id, msgpack.encode(order_entry))
#         logger.info(f"Payment successful for order {order_id}")
#     except redis.exceptions.RedisError:
#         return abort(400, DB_ERROR_STR)

# def handle_payment_failed(data):
#     order_id = data['order_id']
#     order_entry: OrderValue = get_order_from_db(order_id)
#     order_entry.paid = False
#     try:
#         db.set(order_id, msgpack.encode(order_entry))
#         logger.info(f"Payment failed for order {order_id}")
#     except redis.exceptions.RedisError:
#         return abort(400, DB_ERROR_STR)

# def handle_stock_reserved(data):
#     order_id = data['order_id']
#     logger.debug(f"Checkout successful for order {order_id}")
#     return Response("Checkout successful", status=200)

# def handle_stock_failed(data):
#     order_id = data['order_id']
#     order_entry: OrderValue = get_order_from_db(order_id)
#     publish_event('order_events', 'IssueRefund', {
#         'order_id': order_id,
#         'user_id': order_entry.user_id,
#         'total_cost': order_entry.total_cost
#     })
#     logger.info(f"Stock failed for order: {order_id}")

# subscriber_thread = Thread(target=start_subscriber, args=('payment_events', process_event))
# subscriber_thread.start()
# subscriber_thread = Thread(target=start_subscriber, args=('stock_events', process_event))
# subscriber_thread.start()


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
