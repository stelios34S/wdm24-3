import logging
import os
import atexit
import uuid
from threading import Thread
from queue import Queue

import redis

from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response
from event import Event

DB_ERROR_STR = "DB error"

app = Flask("stock-service")
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

db: redis.Redis = redis.Redis(
    host=os.environ['REDIS_HOST'],
    port=int(os.environ['REDIS_PORT']),
    password=os.environ['REDIS_PASSWORD'],
    db=int(os.environ['REDIS_DB'])
)

event_queue = Queue()
def close_db_connection():
    db.close()


atexit.register(close_db_connection)


class StockValue(Struct):
    stock: int
    price: int


def get_item_from_db(item_id: str) -> StockValue | None:
    try:
        entry: bytes = db.get(item_id)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    entry: StockValue | None = msgpack.decode(entry, type=StockValue) if entry else None
    if entry is None:
        abort(400, f"Item: {item_id} not found!")
    return entry


@app.post('/item/create/<price>')
def create_item(price: int):
    key = str(uuid.uuid4())
    app.logger.debug(f"Item: {key} created")
    value = msgpack.encode(StockValue(stock=0, price=int(price)))
    try:
        db.set(key, value)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({'item_id': key})


@app.post('/batch_init/<n>/<starting_stock>/<item_price>')
def batch_init_users(n: int, starting_stock: int, item_price: int):
    n = int(n)
    starting_stock = int(starting_stock)
    item_price = int(item_price)
    kv_pairs: dict[str, bytes] = {f"{i}": msgpack.encode(StockValue(stock=starting_stock, price=item_price))
                                  for i in range(n)}
    try:
        db.mset(kv_pairs)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({"msg": "Batch init for stock successful"})


@app.get('/find/<item_id>')
def find_item(item_id: str):
    item_entry: StockValue = get_item_from_db(item_id)
    return jsonify(
        {
            "stock": item_entry.stock,
            "price": item_entry.price
        }
    )


@app.post('/add/<item_id>/<amount>')
def add_stock(item_id: str, amount: int):
    item_entry: StockValue = get_item_from_db(item_id)
    item_entry.stock += int(amount)
    try:
        db.set(item_id, msgpack.encode(item_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"Item: {item_id} stock updated to: {item_entry.stock}", status=200)


@app.post('/subtract/<item_id>/<amount>')
def remove_stock(item_id: str, amount: int):
    item_entry: StockValue = get_item_from_db(item_id)
    item_entry.stock -= int(amount)
    app.logger.debug(f"Item: {item_id} stock updated to: {item_entry.stock}")
    if item_entry.stock < 0:
        abort(400, f"Item: {item_id} stock cannot get reduced below zero!")
    try:
        db.set(item_id, msgpack.encode(item_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"Item: {item_id} stock updated to: {item_entry.stock}", status=200)

@app.get('/all_stocks')
def all_stocks():
    try:
        keys = db.keys()
        stocks = {}
        for key in keys:
            entry = db.get(key)
            if entry:
                stocks[key.decode('utf-8')] = msgpack.decode(entry, type=StockValue)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({key: {"stock": value.stock, "price": value.price} for key, value in stocks.items()})



def publish_event(event_type, data):
    event = Event(event_type, data)
    logger.info(f"Publishing event: {event.to_json()}")
    db.publish('stock_events', event.to_json())


def handle_event(event):
    data = event.data
    event_type = event.event_type
    if event_type == 'PaymentSuccessful':
        handle_payment_successful(data)


def handle_payment_successful(data):
    order_id = data["order_id"]
    items = data['items']
    for item_id, quantity in items:
        item = get_item_from_db(item_id)
        if not item:
            abort(404, f"Item {item_id} not found")
        if item.stock < quantity:
            publish_event('StockFailed', {'order_id': order_id, 'item_id': item_id, 'amount': quantity, 'user_id': data['user_id'], 'total_cost': data['total_cost']})
            return
        item.stock -= quantity
        try:
            db.set(item_id, msgpack.encode(item))
        except redis.exceptions.RedisError:
            abort(400, DB_ERROR_STR)
    publish_event('StockReserved', {'order_id': order_id, 'items': items, 'user_id': data['user_id'], 'total_cost': data['total_cost']})


# def handle_order_cancelled(data):
#     item_id = data['item_id']
#     amount = data['amount']
#     item = get_item_from_db(item_id)
#     item.stock += amount
#     try:
#         db.set(item_id, msgpack.encode(item))
#     except redis.exceptions.RedisError:
#         abort(400, DB_ERROR_STR)


def listen_to_events():
    pubsub = db.pubsub()
    pubsub.subscribe('payment_events')
    logger.info("Subscribed to stock_events channel.")
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

event_listener_thread = Thread(target=listen_to_events)
event_listener_thread.start()


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
