import logging
import os
import atexit
from threading import Thread
import json
import uuid
import redis
import requests

from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response,g
from rabbitmq_utils import publish_event, start_subscriber

DB_ERROR_STR = "DB error"
REQ_ERROR_STR = "Requests error"

app = Flask("payment-service")
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
GATEWAY_URL = os.environ["GATEWAY_URL"]
logging.getLogger("pika").setLevel(logging.WARNING)


def get_db():
    if 'db' not in g:
        g.db = redis.Redis(host=os.environ['REDIS_HOST'],
                           port=int(os.environ['REDIS_PORT']),
                           password=os.environ['REDIS_PASSWORD'],
                           db=int(os.environ['REDIS_DB']))
    return g.db

@app.teardown_appcontext
def teardown_db(exception):
    db = g.pop('db', None)
    if db is not None:
        db.close()
    if exception:
        logger.error(f"Error in teardown_db: {exception}")

def close_db_connection():
    with app.app_context():
        db =get_db()
        db.close()

atexit.register(close_db_connection)

class UserValue(Struct):
    credit: int



def get_user_from_db(user_id: str) -> UserValue | None:
    try:
        # get serialized data
        db = get_db()
        entry: bytes = db.get(user_id)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    # deserialize data if it exists else return null
    entry: UserValue | None = msgpack.decode(entry, type=UserValue) if entry else None
    if entry is None:
        # if user does not exist in the database; abort
        abort(400, f"User: {user_id} not found!")
    return entry



#@app.post('/create_user') ####transfer to orchestrator
def create_user(data):
    key = data['user_id']
    value = msgpack.encode(UserValue(credit=0))

    try:
        db = get_db()
        db.set(key, value)
        publish_event('events_orchestrator', 'CreateUser', {'correlation_id': key, 'status': 'succeed'})
    except redis.exceptions.RedisError:
        publish_event('events_orchestrator', 'CreateUser', {'correlation_id': key, 'status': 'failed'})
        abort(400, DB_ERROR_STR)


#@app.post('/batch_init/<n>/<starting_money>')
def batch_init_users(data):
    n = data["n"]
    key = data["key"]
    starting_money = data["starting_money"]
    starting_money = int(starting_money)
    kv_pairs: dict[str, bytes] = {f"{i}": msgpack.encode(UserValue(credit=starting_money)) for i in range(n)}
    try:
        db = get_db()
        db.mset(kv_pairs)
        publish_event('events_orchestrator', 'BatchInit', {'correlation_id': "BatchInitPayment"+key, 'status': 'succeed'})
    except redis.exceptions.RedisError:
        publish_event('events_orchestrator', 'BatchInit', {'correlation_id': "BatchInitPayment"+key, 'status': 'failed'})
        abort(400, DB_ERROR_STR)


#@app.get('/find_user/<user_id>')
def find_user(data):
    user_id = data['user_id']
    try:
        user_entry: UserValue = get_user_from_db(user_id)
        publish_event('events_orchestrator', 'FindUser', {'correlation_id': user_id, 'status': 'succeed', 'credit': user_entry.credit})
    except redis.exceptions.RedisError:
        publish_event('events_orchestrator', 'FindUser', {'correlation_id': user_id, 'status': 'failed'})
        abort(400, DB_ERROR_STR)



#@app.post('/add_funds/<user_id>/<amount>') #####transfer to orchestrator
def add_credit(data):
    user_id = data["user_id"]
    amount = data["amount"]
    user_entry: UserValue = get_user_from_db(user_id)
    # update credit, serialize and update database
    user_entry.credit += int(amount)
    try:
        db = get_db()
        db.set(user_id, msgpack.encode(user_entry))
        publish_event('events_orchestrator', 'AddCredit', {'correlation_id': user_id, 'status': 'succeed'})
    except redis.exceptions.RedisError:
        publish_event('events_orchestrator', 'AddCredit', {'correlation_id': user_id, 'status': 'failed'})
        abort(400, DB_ERROR_STR)



#@app.post('/pay/<user_id>/<amount>') #####transfer to orchestrator
def remove_credit(data):
    user_id= data["user_id"]
    amount = data["amount"]
    logger.info(f"Removing {amount} credit from user: {user_id}")
    user_entry: UserValue = get_user_from_db(user_id)
    # update credit, serialize and update database
    user_entry.credit -= int(amount)
    if user_entry.credit < 0:
        publish_event('events_orchestrator', 'RemoveCredit', {'correlation_id': user_id, 'status': 'failed'})
        abort(400, f"User: {user_id} credit cannot get reduced below zero!")
    try:
        db = get_db()
        db.set(user_id, msgpack.encode(user_entry))
        publish_event('events_orchestrator', 'RemoveCredit', {'correlation_id': user_id, 'status': 'succeed'})
    except redis.exceptions.RedisError:
        publish_event('events_orchestrator', 'RemoveCredit', {'correlation_id': user_id, 'status': 'failed'})
        abort(400, DB_ERROR_STR)


def handle_process_payment(data):
    user_id = data['user_id']
    order_id = data['order_id']
    total_cost = data['total_cost']
    items = data['items']
    user_entry: UserValue = get_user_from_db(user_id)
    if user_entry.credit >= total_cost:
        user_entry.credit -= total_cost
        try:
            db = get_db()
            db.set(user_id, msgpack.encode(user_entry))

            publish_event('events_order', 'PaymentSuccessfulOrder', {
                'order_id': order_id,
                'user_id': user_id,
                'total_cost': total_cost,
                'items': items
            })
            publish_event('events_stock', 'PaymentSuccessfulStock', {
                'order_id': order_id,
                'user_id': user_id,
                'total_cost': total_cost,
                'items': items
            })
            logger.info(f"Payment successful for order: {order_id}")
        except redis.exceptions.RedisError:
            publish_event('events_order', 'PaymentFailed', {'order_id': order_id})
            abort(400, DB_ERROR_STR)
    else:
        publish_event('events_order', 'PaymentFailed', {
            'order_id': order_id,
        })
        logger.info(f"Payment failed for order: {order_id}")
        #abort(400, "Insufficient credit")

def handle_issue_refund(data): #####maybe change order id to userid
    user_id = data['user_id']
    order_id = data['order_id']
    total_cost = data['total_cost']
    user_entry: UserValue = get_user_from_db(user_id)
    user_entry.credit += total_cost
    try:
        db = get_db()
        db.set(user_id, msgpack.encode(user_entry))
        publish_event('events_order', 'RefundIssued', {
            'order_id': order_id,
        })
        logger.info(f"Refund issued for order: {order_id}")
    except redis.exceptions.RedisError:
        publish_event('events_orchestrator', 'IssueRefund', {'correlation_id': order_id, 'status': 'failed'})
        abort(400, DB_ERROR_STR)


def process_event(ch, method, properties, body):
    with app.app_context():
        event = json.loads(body)
        event_type = event['type']
        app.logger.info(event_type)
        data = event['data']
        if event_type == "ProcessPayment":
            handle_process_payment(data)
        elif event_type == "IssueRefund":
            handle_issue_refund(data)
        elif event_type == "CreateUser":
            create_user(data)
        elif event_type == "AddCredit":
            add_credit(data)
        elif event_type == "RemoveCredit":
            remove_credit(data)
        elif event_type == "BatchInit":
            batch_init_users(data)
        elif event_type == "FindUser":
            find_user(data)

start_subscriber('events_payment', process_event)

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)