import logging
import os
import atexit
from threading import Thread
import json
import uuid
import redis
import requests

from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response
from rabbitmq_utils import publish_event, start_subscriber

DB_ERROR_STR = "DB error"


app = Flask("payment-service")
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
GATEWAY_URL = "http://orchestrator:8001"
logging.getLogger("pika").setLevel(logging.WARNING)

db: redis.Redis = redis.Redis(host=os.environ['REDIS_HOST'],
                              port=int(os.environ['REDIS_PORT']),
                              password=os.environ['REDIS_PASSWORD'],
                              db=int(os.environ['REDIS_DB']))


def close_db_connection():
    db.close()

# event_queue = Queue()


atexit.register(close_db_connection)

class UserValue(Struct):
    credit: int



def send_post_request_orch(url, data):
    try:
        response = requests.post(url, json=data)
        response.raise_for_status()
        logger.info(f"Sent POST request to {url} with data {data}")
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to send POST request to {url}: {e}")
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


#@app.post('/create_user') ####transfer to orchestrator
def create_user(data):
    key = data['user_id']
    value = msgpack.encode(UserValue(credit=0))

    try:
        db.set(key, value)
    except redis.exceptions.RedisError:
        send_post_request_orch(f"{GATEWAY_URL}/acks", json.dumps({'type': 'CreateUser', 'status': 'failed','correlation_id': key}))
        abort(400, DB_ERROR_STR)
        #return abort(400, DB_ERROR_STR)
    send_post_request_orch(f"{GATEWAY_URL}/acks", json.dumps({'type': 'CreateUser', 'status': 'succeed','correlation_id': key}))
    #return jsonify({'user_id': key})


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


#@app.post('/add_funds/<user_id>/<amount>') #####transfer to orchestrator
def add_credit(data):
    user_id = data["user_id"]
    amount = data["amount"]
    user_entry: UserValue = get_user_from_db(user_id)
    # update credit, serialize and update database
    user_entry.credit += int(amount)
    try:
        db.set(user_id, msgpack.encode(user_entry))
    except redis.exceptions.RedisError:
        send_post_request_orch(f"{GATEWAY_URL}/acks", json.dumps({'type': 'AddCredit', 'status': 'failed','correlation_id': user_id}))
        abort(400, DB_ERROR_STR)
    send_post_request_orch(f"{GATEWAY_URL}/acks", json.dumps({'type': 'AddCredit', 'status': 'succeed','correlation_id': user_id}))
    #return Response(f"User: {user_id} credit updated to: {user_entry.credit}", status=200)


#@app.post('/pay/<user_id>/<amount>') #####transfer to orchestrator
def remove_credit(data):
    user_id= data["user_id"]
    amount = data["amount"]
    logger.info(f"Removing {amount} credit from user: {user_id}")
    user_entry: UserValue = get_user_from_db(user_id)
    # update credit, serialize and update database
    user_entry.credit -= int(amount)
    if user_entry.credit < 0:
        send_post_request_orch(f"{GATEWAY_URL}/acks", json.dumps({'type': 'RemoveCredit', 'status': 'failed','correlation_id': user_id}))
        abort(400, f"User: {user_id} credit cannot get reduced below zero!")
    try:
        db.set(user_id, msgpack.encode(user_entry))
    except redis.exceptions.RedisError:
        send_post_request_orch(f"{GATEWAY_URL}/acks", json.dumps({'type': 'RemoveCredit', 'status': 'failed','correlation_id': user_id}))
        abort(400, DB_ERROR_STR)
    send_post_request_orch(f"{GATEWAY_URL}/acks", json.dumps({'type': 'RemoveCredit', 'status': 'succeed','correlation_id': user_id}))
    #return Response(f"User: {user_id} credit updated to: {user_entry.credit}", status=200)

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
            abort(400, DB_ERROR_STR)
        publish_event('events', 'PaymentSuccessfulOrder', {
            'order_id': order_id,
            'user_id': user_id,
            'total_cost': total_cost,
            'items': items
        })
        publish_event('events', 'PaymentSuccessfulStock', {
            'order_id': order_id,
            'user_id': user_id,
            'total_cost': total_cost,
            'items': items
        })
        #send_post_request_orch(f"{GATEWAY_URL}/acks", json.dumps({'type': 'Checkout', 'status': 'payment_successful'}))
        logger.info(f"Payment successful for order: {order_id}")
    else:
        publish_event('events', 'PaymentFailed', {
            'order_id': order_id,
        })
        logger.info(f"Payment failed for order: {order_id}")
        abort(400, "Insufficient credit")

def handle_issue_refund(data):
    user_id = data['user_id']
    order_id = data['order_id']
    total_cost = data['total_cost']
    user_entry: UserValue = get_user_from_db(user_id)
    user_entry.credit += total_cost
    try:
        db.set(user_id, msgpack.encode(user_entry))
    except redis.exceptions.RedisError:
        send_post_request_orch(f"{GATEWAY_URL}/acks", json.dumps({'type': 'IssueRefund', 'status': 'failed','correlation_id': order_id}))
        abort(400, DB_ERROR_STR)
    publish_event('events', 'RefundIssued', {
        'order_id': order_id,
    })
    #send_post_request_orch(f"{GATEWAY_URL}/acks", json.dumps({'type': 'IssueRefund', 'status': 'succeed'}))
    logger.info(f"Refund issued for order: {order_id}")


def process_event(ch, method, properties, body):
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



start_subscriber('events', process_event)


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
