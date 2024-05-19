import logging
import os
import atexit
import uuid
from threading import Thread

import redis

from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response
from event import Event

DB_ERROR_STR = "DB error"

app = Flask("payment-service")
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

db: redis.Redis = redis.Redis(host=os.environ['REDIS_HOST'],
                              port=int(os.environ['REDIS_PORT']),
                              password=os.environ['REDIS_PASSWORD'],
                              db=int(os.environ['REDIS_DB']))


def close_db_connection():
    db.close()


atexit.register(close_db_connection)


class UserValue(Struct):
    credit: int


def get_user_from_db(user_id: str) -> UserValue | None:
    try:
        entry: bytes = db.get(user_id)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    entry: UserValue | None = msgpack.decode(entry, type=UserValue) if entry else None
    if entry is None:
        abort(400, f"User: {user_id} not found!")
    return entry


@app.post('/create_user')
def create_user():
    key = str(uuid.uuid4())
    value = msgpack.encode(UserValue(credit=0))
    try:
        db.set(key, value)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({'user_id': key})


@app.post('/batch_init/<n>/<starting_money>')
def batch_init_users(n: int, starting_money: int):
    n = int(n)
    starting_money = int(starting_money)
    kv_pairs: dict[str, bytes] = {f"{i}": msgpack.encode(UserValue(credit=starting_money))
                                  for i in range(n)}
    try:
        db.mset(kv_pairs)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({"msg": "Batch init for users successful"})


@app.get('/find_user/<user_id>')
def find_user(user_id: str):
    user_entry: UserValue = get_user_from_db(user_id)
    return jsonify(
        {
            "user_id": user_id,
            "credit": user_entry.credit
        }
    )


@app.post('/add_funds/<user_id>/<amount>')
def add_credit(user_id: str, amount: int):
    user_entry: UserValue = get_user_from_db(user_id)
    user_entry.credit += int(amount)
    try:
        db.set(user_id, msgpack.encode(user_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"User: {user_id} credit updated to: {user_entry.credit}", status=200)


@app.post('/pay/<user_id>/<amount>')
def remove_credit(user_id: str, amount: int):
    app.logger.debug(f"Removing {amount} credit from user: {user_id}")
    user_entry: UserValue = get_user_from_db(user_id)
    user_entry.credit -= int(amount)
    if user_entry.credit < 0:
        abort(400, f"User: {user_id} credit cannot get reduced below zero!")
    try:
        db.set(user_id, msgpack.encode(user_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    publish_event("PaymentCompleted", {"order_id": request.json["order_id"], "user_id": user_id})
    return Response(f"User: {user_id} credit updated to: {user_entry.credit}", status=200)


def publish_event(event_type, data):
    event = Event(event_type, data)
    logger.info(f"Publishing event: {event.to_json()}")
    db.publish('payment_events', event.to_json())


def handle_event(event):
    data = event.data
    event_type = event.event_type
    if event_type == "OrderPayment":
        handle_order_payment(data)


def handle_order_payment(data):
    user_id = data["user_id"]
    order_id = data["order_id"]
    amount = data["amount"]
    user_entry = get_user_from_db(user_id)
    if user_entry.credit >= amount:
        user_entry.credit -= amount
        try:
            db.set(user_id, msgpack.encode(user_entry))
        except redis.exceptions.RedisError:
            logger.error(f"Failed to update user {user_id} credit")
        else:
            logger.info(f"User {user_id} credit deducted by {amount}")
            publish_event("PaymentCompleted", {"order_id": order_id, "user_id": user_id})
    else:
        logger.info(f"User {user_id} out of credit")
        publish_event("PaymentFailed", {"order_id": order_id, "user_id": user_id})


def subscribe_to_events():
    pubsub = db.pubsub()
    pubsub.subscribe('order_events')
    logger.info("Subscribed to order_events channel.")
    for message in pubsub.listen():
        if message['type'] == 'message':
            event = Event.from_json(message['data'])
            handle_event(event)


subscriber_thread = Thread(target=subscribe_to_events)
subscriber_thread.start()


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
