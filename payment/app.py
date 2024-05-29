import logging
import os
import atexit
from threading import Thread
import json
import uuid
import redis

from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response
from rabbitmq_utils import publish_event, start_subscriber

DB_ERROR_STR = "DB error"


app = Flask("payment-service")
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

logging.getLogger("pika").setLevel(logging.WARNING)

db: redis.Redis = redis.Redis(host=os.environ['REDIS_HOST'],
                              port=int(os.environ['REDIS_PORT']),
                              password=os.environ['REDIS_PASSWORD'],
                              db=int(os.environ['REDIS_DB']))

# pubsub = db.pubsub()
# pubsub.subscribe('order_events')
#
# logger.info(db.pubsub_channels())
def close_db_connection():
    db.close()

# event_queue = Queue()

# def subscribe_to_events():
#     logger.info("Subscribed to order-events channel.")
#     for message in pubsub.listen():
#         logger.info(f"Received message: {message}")
#         if message['type'] != 'subscribe':
#             event = Event.from_json(message['data'])
#             handle_event(event)


# subscriber_thread = Thread(target=subscribe_to_events)
# subscriber_thread.start()


atexit.register(close_db_connection)

class UserValue(Struct):
    credit: int

# def publish_event(event_type, data):
#     event = Event(event_type, data)
#     logger.info(f"Publishing event: {event.to_json()}")
#     db.publish('payment_events', event.to_json())


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


@app.post('/pay/<user_id>/<amount>')
def remove_credit(user_id: str, amount: int):
    logger.info(f"Removing {amount} credit from user: {user_id}")
    user_entry: UserValue = get_user_from_db(user_id)
    # update credit, serialize and update database
    user_entry.credit -= int(amount)
    if user_entry.credit < 0:
        abort(400, f"User: {user_id} credit cannot get reduced below zero!")
    try:
        db.set(user_id, msgpack.encode(user_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"User: {user_id} credit updated to: {user_entry.credit}", status=200)

def handle_process_payment(data):
    logger.info("Process payment")
    user_id = data['user_id']
    order_id = data['order_id']
    total_cost = data['total_cost']
    items = data['items']
    user_entry: UserValue = get_user_from_db(user_id)
    if user_entry.credit >= total_cost:
        user_entry.credit -= total_cost
        try:
            db.set(user_id, msgpack.encode(user_entry))
        except redis.exceptions.RedisError:
            return abort(400, DB_ERROR_STR)
        publish_event('payment_events', 'PaymentSuccessful', {
            'order_id': order_id,
            'user_id': user_id,
            'total_cost': total_cost,
            'items': items
        })
        logger.info(f"Payment successful for order: {order_id}")
    else:
        publish_event('payment_events', 'PaymentFailed', {
            'order_id': order_id,
        })
        logger.info(f"Payment failed for order: {order_id}")

def handle_issue_refund(data):
    user_id = data['user_id']
    order_id = data['order_id']
    total_cost = data['total_cost']
    user_entry: UserValue = get_user_from_db(user_id)
    user_entry.credit += total_cost
    try:
        db.set(user_id, msgpack.encode(user_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    publish_event('payment_events', 'RefundIssued', {
        'order_id': order_id,
    })
    logger.info(f"Refund issued for order: {order_id}")

# def handle_event(event):
#     data = event.data
#     event_type = event.event_type
#     logger.info(f"Event: {event}")
#     if event_type == "ProcessPayment":
#         logger.info("Received event")
#         handle_process_payment(data)
#     elif event_type == "IssueRefund":
#         handle_issue_refund(data)
#     else:
#         logger.info("You should not be here")

def process_event(ch, method, properties, body):
    event = json.loads(body)
    event_type = event['type']
    app.logger.info(event_type)
    data = event['data']
    if event_type == "ProcessPayment":
        handle_process_payment(data)
    elif event_type == "IssueRefund":
        handle_issue_refund(data)

# def process_event_queue():
#     while True:
#         logger.debug(event_queue.get())
#         event = event_queue.get()
#         logger.info(f"Event: {event}")
#         handle_event(event)
#         event_queue.task_done()
#
#
# # Start a few worker threads
# for i in range(5):
#     worker = Thread(target=process_event_queue)
#     worker.daemon = True
#     worker.start()

subscriber_thread = Thread(target=start_subscriber, args=('order_events', process_event))
subscriber_thread.start()


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
