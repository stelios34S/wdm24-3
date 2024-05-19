import logging
import os
import atexit
import uuid
import requests
import threading

import redis

from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response
from event import Event

DB_ERROR_STR = "DB error"
REQ_ERROR_STR = "Requests error"
GATEWAY_URL = os.environ['GATEWAY_URL']

app = Flask("payment-service")

# Set up the logger
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
        # get serialized data
        entry: bytes = db.get(user_id)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    # deserialize data if it exists else return null
    entry: UserValue | None = msgpack.decode(entry, type=UserValue) if entry else None
    if entry is None:
        # if user does not exist in the database; abort
        abort(400, f"User: {user_id} not found!")
    return entry


def subscribe_to_events():
    pubsub = db.pubsub()
    pubsub.subscribe('order-events')
    logger.info("Subscribed to order-events channel.")
    for message in pubsub.listen():
        logger.info(f"Received message: {message}")
        if message['type'] == 'message':
            event = Event.from_json(message['data'])
            handle_event(event)

subscriber_thread = threading.Thread(target=subscribe_to_events)
subscriber_thread.start()


def handle_event(event):
    data = event.data
    event_type = event.event_type
    if event_type == "OrderUpdated":
        user_id = data["user_id"]
        order_id = data["order_id"]
        amount = data["amount"]
        logger.info(f"Processing OrderUpdated event for order_id: {order_id}")
        process_payment(user_id, order_id, amount)
    elif event_type == "OrderFailed":
        user_id = data["user_id"]
        order_id = data["order_id"]
        amount = data["amount"]
        logger.info(f"Processing OrderFailed event for order_id: {order_id}")
        rollback_payment(user_id, amount)


def publish_event(event_type, data):
    event = Event(event_type, data)
    logger.info("Publishing event")
    db.publish('order-events', event.to_json())


def send_post_request(url: str):
    try:
        response = requests.post(url)
    except requests.exceptions.RequestException:
        abort(400, REQ_ERROR_STR)
    else:
        return response


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
    # update credit, serialize and update database
    user_entry.credit += int(amount)
    try:
        db.set(user_id, msgpack.encode(user_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"User: {user_id} credit updated to: {user_entry.credit}", status=200)


def rollback_payment(user_id: str, amount: int):
    user_entry: UserValue = get_user_from_db(user_id)
    user_entry.credit += amount
    try:
        db.set(user_id, msgpack.encode(user_entry))
    except redis.exceptions.RedisError:
        app.logger.error(f"Failed to rollback payment for user: {user_id}")


@app.post('/pay/<user_id>/<amount>')
def process_payment(user_id: str, order_id: str, amount: int):
    app.logger.debug(f"Processing payment of {amount} for user: {user_id}, order: {order_id}")
    user_entry: UserValue = get_user_from_db(user_id)
    amount = int(amount)
    if user_entry.credit < amount:
        abort(400, f"User: {user_id} does not have enough credit!")
    user_entry.credit -= amount

    try:
        # remove credit
        db.set(user_id, msgpack.encode(user_entry))

        # update order status
        response = send_post_request(f"{GATEWAY_URL}/orders/checkout/{order_id}")
        if response.status_code != 200:
            rollback_payment(user_id, amount)
            raise Exception(REQ_ERROR_STR)

        # successful payment
        db.set(f"{user_id}:{order_id}", True)


    except redis.exceptions.RedisError:

        rollback_payment(user_id, amount)

        return abort(400, DB_ERROR_STR)

    except Exception as e:

        app.logger.error(f"Failed to update services: {str(e)}")

        return abort(400, str(e))

    return Response(f"User: {user_id} credit updated to: {user_entry.credit}", status=200)


'''
To do: 
/payment/cancel/{user_id}/{order_id}
/payment/status/{user_id}/{order_id}
'''

@app.post('/cancel/<user_id>/<order_id>')
def cancel_payment(user_id: str, order_id: str):
    try:
        db.delete(f"{user_id}:{order_id}")
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({"done": True})


@app.get('/status/<user_id>/<order_id>')
def payment_status(user_id: str, order_id: str):
    try:
        paid = db.get(f"{user_id}:{order_id}")
        if paid is None:
            return jsonify({"paid": False})
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({"paid": True})



if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
