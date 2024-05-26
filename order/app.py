import logging
import os
import atexit
import random
import uuid
from collections import defaultdict
from threading import Thread
from queue import Queue
import redis
import requests

from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response
from event import Event

DB_ERROR_STR = "DB error"
REQ_ERROR_STR = "Requests error"

GATEWAY_URL = os.environ['GATEWAY_URL']

app = Flask("order-service")
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

db: redis.Redis = redis.Redis(host=os.environ['REDIS_HOST'],
                              port=int(os.environ['REDIS_PORT']),
                              password=os.environ['REDIS_PASSWORD'],
                              db=int(os.environ['REDIS_DB']))

event_queue = Queue()
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
        entry: bytes = db.get(order_id)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    entry: OrderValue | None = msgpack.decode(entry, type=OrderValue) if entry else None
    if entry is None:
        abort(400, f"Order: {order_id} not found!")
    return entry


@app.post('/create/<user_id>')
def create_order(user_id: str):
    key = str(uuid.uuid4())
    value = msgpack.encode(OrderValue(paid=False, items=[], user_id=user_id, total_cost=0))
    try:
        db.set(key, value)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    publish_event("OrderCreated", {"order_id": key, "user_id": user_id})
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
                           total_cost=2 * item_price)
        return value

    kv_pairs: dict[str, bytes] = {f"{i}": msgpack.encode(generate_entry())
                                  for i in range(n)}
    try:
        db.mset(kv_pairs)
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
        abort(400, f"Item: {item_id} does not exist!")
    item_json: dict = item_reply.json()
    order_entry.items.append((item_id, int(quantity)))
    order_entry.total_cost += int(quantity) * item_json["price"]
    try:
        db.set(order_id, msgpack.encode(order_entry))
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
    items_quantities: dict[str, int] = defaultdict(int)
    for item_id, quantity in order_entry.items:
        items_quantities[item_id] += quantity
    removed_items: list[tuple[str, int]] = []
    for item_id, quantity in items_quantities.items():
        stock_reply = send_post_request(f"{GATEWAY_URL}/stock/subtract/{item_id}/{quantity}")
        if stock_reply.status_code != 200:
            rollback_stock(removed_items)
            abort(400, f'Out of stock on item_id: {item_id}')
        removed_items.append((item_id, quantity))
    user_reply = send_post_request(f"{GATEWAY_URL}/payment/pay/{order_entry.user_id}/{order_entry.total_cost}")
    if user_reply.status_code != 200:
        rollback_stock(removed_items)
        abort(400, "User out of credit")
    order_entry.paid = True
    try:
        db.set(order_id, msgpack.encode(order_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    app.logger.debug("Checkout successful")
    return Response("Checkout successful", status=200)


def publish_event(event_type, data):
    event = Event(event_type, data)
    logger.info(f"Publishing event: {event.to_json()}")
    db.publish('order_events', event.to_json())


def handle_event(event):
    data = event.data
    event_type = event.event_type
    if event_type == "PaymentSuccessful":
        handle_payment_completed(data)
    elif event_type == "PaymentFailed":
        handle_payment_failed(data)
    elif event_type == "StockReserved":
        handle_stock_reserved(data)
    elif event_type == "StockFailed":
        handle_stock_failed(data)
    elif event_type == "RefundIssued":
        handle_refund_issued(data)


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
    # Inform the user and update the order status, no event to publish
    # Here you can add code to inform the user
    # e.g., send an email or update order status in a UI


def handle_payment_failed(data):
    order_id = data["order_id"]
    order_entry = get_order_from_db(order_id)
    if order_entry:
        logger.info(f"Order {order_id} payment failed")
        # Inform the user about the payment failure
        # e.g., send an email or update order status in a UI


def handle_stock_reserved(data):
    order_id = data["order_id"]
    logger.info(f"Stock reserved for order {order_id}")
    order_entry = get_order_from_db(order_id)
    if order_entry.paid:
        try:
            db.set(order_id, msgpack.encode(order_entry))
        except redis.exceptions.RedisError:
            logger.error(f"Failed to update order {order_id} status to paid")
        else:
            logger.info(f"Order {order_id} marked as paid")


def handle_stock_failed(data):
    order_id = data["order_id"]
    logger.info(f"Order {order_id} stock reservation failed")
    publish_event("IssueRefund", {"order_id": order_id, "user_id": data["user_id"], "amount": data["total_cost"]})

def handle_refund_issued(data):
    order_id = data["order_id"]
    logger.info(f"Refund issued for order {order_id}")
    # Inform the user about the refund

def subscribe_to_events():
    pubsub = db.pubsub()
    pubsub.subscribe(['payment_events', 'stock_events'])
    logger.info("Subscribed to payment_events and stock_events channels.")
    for message in pubsub.listen():
        if message['type'] == 'message':
            event = Event.from_json(message['data'])
            event_queue.put(event)





def process_event_queue():
    while True:
        event = event_queue.get()
        handle_event(event)
        event_queue.task_done()
# Start a few worker threads
for i in range(5):
    worker = Thread(target=process_event_queue)
    worker.daemon = True
    worker.start()

subscriber_thread = Thread(target=subscribe_to_events)
subscriber_thread.start()

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
