import logging
import os
import atexit
import random
import json
from threading import Thread
import uuid
from queue import Queue, Empty
import time
import redis
import requests

from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response, request, g
from rabbitmq_utils import publish_event, start_subscriber

DB_ERROR_STR = "DB error"
REQ_ERROR_STR = "Requests error"

GATEWAY_URL = os.environ['GATEWAY_URL']
ACK_TIMEOUT = 30  # seconds

# Dictionary to hold queues for different correlation IDs

app = Flask("orchestrator")
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

logging.getLogger("pika").setLevel(logging.WARNING)


###########WE DO HTTPS REQUESTS TO ORCHESTRATOR
############## ORCHESTRATOR SENDS MESSAGES  TO SERVICES,
############## SERVICES SEND MESSAGES TO EACH OTHER,
############## AND THE FINAL SERVICE THAT FINISHES ITS JOB, SENDS HTTP REQUEST TO ORCHESTRATOR
############## SO ORCHESTARTOR TAKES THE FINAL ACK MESSAGE FROM THE SERVICE THAT IT FINISHES
############### AND SENDS IT BACK TO THE USER AS A RESPONSE


# ----------------------------------ORCHESTRATOR-------------------------------------------------------------
def get_db():
    if 'db' not in g:
        g.db = redis.Redis(host=os.environ['REDIS_HOST'], port=int(os.environ['REDIS_PORT']),
                           password=os.environ['REDIS_PASSWORD'], db=int(os.environ['REDIS_DB']))
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

def store_ack(correlation_id, ack_data):
    db = get_db()
    db.setex(correlation_id, ACK_TIMEOUT, json.dumps(ack_data))
def retrieve_ack(correlation_id):
    db = get_db()
    ack_data = db.get(correlation_id)
    return json.loads(ack_data) if ack_data else None

def process_event(body):
    correlation_id = body['data'].get('correlation_id')
    if correlation_id:
        store_ack(correlation_id, body)

# -------------------------------------------------------------------------------------------------------------
# ----------------------------------ORDER SERVICE-------------------------------------------------------------
@app.post('/orders/create/<user_id>')
def create_order(user_id: str):
    try:
        key = str(uuid.uuid4())
        data = {"order_id": key, "user_id": user_id}
        publish_event("events_order", "OrderCreation", data)
        # logger.info(f"Order created: {key}, for user {user_id}")
        ###await for ack in queue to return response to user (200 or 400)
        ##create order success or failure
        start_subscriber('events_orchestrator', process_event, key)
        ack_data = retrieve_ack(key)
        logger.info(f'message received with ACK{ack_data}')
        ##ack = await_ack(key)
        if ack_data.get("type") == "CreateOrder" and ack_data['data'].get('status') == 'succeed':
            return jsonify({'order_id': key})
        else:
            return abort(400, DB_ERROR_STR)
    except TimeoutError:
        return abort(400, REQ_ERROR_STR)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)


@app.post('/orders/checkout/<order_id>')
def checkout(order_id: str):
    try:
        logger.debug(f"Checking out {order_id}")
        data = {"order_id": order_id}
        publish_event("events_order", "Checkout", data)
        logger.info("Checkout initiated")
        start_subscriber('events_orchestrator', process_event, order_id)
        ack_data = retrieve_ack(order_id)
        if ack_data.get('type') == 'Checkout' and ack_data['data'].get('status') == 'succeed':
            return Response("Checkout succeed", status=200)
        if ack_data.get('type') == 'IssueRefund' and ack_data['data'].get('status') == 'succeed':
            return Response("Refund issued", status=200)
        else:
            abort(400, DB_ERROR_STR)
    except redis.exceptions.RedisError:
        ###await for ack in queue to return response to user (200 or 400) if checkout is successful return 200
        ### or expects an issuerefund message with succeed or failure
        return abort(400, REQ_ERROR_STR)


@app.post('/orders/addItem/<order_id>/<item_id>/<quantity>')
def add_item(order_id: str, item_id: str, quantity: int):
    try:

        data = {"order_id": order_id, "item_id": item_id, "quantity": quantity}
        publish_event("events_order", "AddItemCheck", data)
        start_subscriber('events_orchestrator', process_event, order_id)
        ack_data = retrieve_ack(order_id)
        if ack_data.get('type') == 'ItemAdded' and ack_data['data'].get('status') == 'succeed':
            return Response("Item added to order", status=200)
        else:
            return abort(400, DB_ERROR_STR)
        ###await for ack in queue to return response to user (200 or 400)
        ###additem message success or failure
    except TimeoutError:
        return abort(400, REQ_ERROR_STR)
    except redis.exceptions.RedisError:
        return abort(400, REQ_ERROR_STR)


@app.post('/orders/batch_init/<n>/<n_items>/<n_users>/<item_price>')
def batch_init_users_orders(n: int, n_items: int, n_users: int, item_price: int):
    # logger.info(f"Batch init  n: {n}, n_items: {n_items}, n_users: {n_users}, item_price: {item_price}")
    try:

        data = {"n": int(n), "n_items": int(n_items), "n_users": int(n_users), "item_price": int(item_price)}
        publish_event("events_order", "BatchInit", data)
        start_subscriber('events_orchestrator', process_event, "BatchInitOrders")
        ack_data = retrieve_ack("BatchInitOrders")
        if ack_data.get('type') == 'BatchInit' and ack_data['data'].get('status') == 'succeed':
            logger.info("Batch init for orders successful")
            return Response("Batch init successful", status=200)
        else:
            return abort(400, DB_ERROR_STR)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)


@app.get('/orders/find/<order_id>')  ###Does not need to get transferred
def find_order(order_id: str):
    try:

        ack_data = None
        data = {"order_id": order_id}
        publish_event("events_order", "FindOrder", data)
        start_subscriber('events_orchestrator', process_event, order_id)
        ack_data = retrieve_ack(order_id)
        if ack_data.get('type') == 'FindOrder' and ack_data['data'].get('status') == 'succeed':
            return jsonify(
                {
                    "order_id": order_id,
                    "paid": ack_data['data']['paid'],
                    "items": ack_data['data']['items'],
                    "user_id": ack_data['data']['user_id'],
                    "total_cost": ack_data['data']['total_cost']
                }
            )
        else:
            return abort(400, DB_ERROR_STR)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)


# ------------------------------------------------------------------------------------------------------------------
# ----------------------------------PAYMENT SERVICE-------------------------------------------------------------
@app.post('/payment/create_user')
def create_user():
    try:
        key = str(uuid.uuid4())
        data = {"user_id": key}
        publish_event("events_payment", "CreateUser", data)
        start_subscriber('events_orchestrator', process_event, key)
        ack_data = retrieve_ack(key)
        if ack_data.get('type') == 'CreateUser' and ack_data["data"].get('status') == 'succeed':
            logger.info(f"User: {key} created")
            return jsonify({'user_id': key})
        else:
            return abort(400, DB_ERROR_STR)
        ###await for ack in queue to return response to user (200 or 400)
        ##create user message success or failure
    except TimeoutError:
        return abort(400, REQ_ERROR_STR)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)


@app.post('/payment/add_funds/<user_id>/<amount>')
def add_credit(user_id: str, amount: int):
    try:

        data = {"user_id": user_id, "amount": amount}
        publish_event("events_payment", "AddCredit", data)
        start_subscriber('events_orchestrator', process_event, user_id)
        ack_data = retrieve_ack(user_id)
        if ack_data.get('type') == 'AddCredit' and ack_data['data'].get('status') == 'succeed':
            return Response(f"User: {user_id} credit is beging updated", status=200)
        else:
            return abort(400, DB_ERROR_STR)
        ###await for ack in queue to return response to user (200 or 400)
        ###addcredit message success or     failure
    except TimeoutError:
        return abort(400, REQ_ERROR_STR)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)


@app.post('/payment/pay/<user_id>/<amount>')
def remove_credit(user_id: str, amount: int):
    logger.info(f"Removing {amount} credit from user: {user_id}")
    try:

        data = {"user_id": user_id, "amount": amount}
        publish_event("events_payment", "RemoveCredit", data)
        start_subscriber('events_orchestrator', process_event, user_id)
        ack_data = retrieve_ack(user_id)
        if ack_data.get('type') == 'RemoveCredit' and ack_data['data'].get('status') == 'succeed':
            return Response(f"User: {user_id} credit is  updated", status=200)
        else:
            return abort(400, DB_ERROR_STR)
        ###await for ack in queue to return response to user (200 or 400)
        ### removecredit message success or failure
    except TimeoutError:
        return abort(400, REQ_ERROR_STR)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)


@app.post('/payment/batch_init/<n>/<starting_money>')
def batch_init_users_payment(n: int, starting_money: int):
    try:

        ack_data = None
        data = {"n": int(n), "starting_money": int(starting_money)}
        publish_event("events_payment", "BatchInit", data)
        start_subscriber('events_orchestrator', process_event, "BatchInitPayment")
        ack_data = retrieve_ack("BatchInitPayment")
        if ack_data.get('type') == 'BatchInit' and ack_data['data'].get('status') == 'succeed':
            logger.info("Batch init for payment successful")
            return Response("Batch init successful", status=200)
        else:
            return abort(400, DB_ERROR_STR)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)


@app.get('/payment/find_user/<user_id>')
def find_user(user_id: str):
    try:
        data = {"user_id": user_id}
        publish_event("events_payment", "FindUser", data)
        start_subscriber('events_orchestrator', process_event, user_id)
        ack_data = retrieve_ack(user_id)
        if ack_data.get('type') == 'FindUser' and ack_data['data'].get('status') == 'succeed':
            return jsonify(
                {
                    "user_id": user_id,
                    "credit": ack_data['data']['credit']
                }
            )
        else:
            return abort(400, DB_ERROR_STR)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)


# ------------------------------------------------------------------------------------------------------------------
# ----------------------------------STOCK SERVICE-------------------------------------------------------------

@app.post('/stock/item/create/<price>')
def create_item(price: int):
    try:

        key = str(uuid.uuid4())
        data = {"price": price, "item_id": key}
        publish_event("events_stock", "CreateItem", data)
        ###await for ack in queue to return response to user (200 or 400)
        start_subscriber('events_orchestrator', process_event, key)
        ack_data = retrieve_ack(key)
        if ack_data.get('type') == 'CreateItem' and ack_data['data'].get('status') == 'succeed':
            logger.info(f"Item: {key} created")
            return jsonify({'item_id': key})
        else:
            return abort(400, DB_ERROR_STR)
    except TimeoutError:
        return abort(400, REQ_ERROR_STR)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)


@app.post('/stock/add/<item_id>/<amount>')  ####Transfered to orchestrator
def add_stock(item_id: str, amount: int):
    try:
        ack_data = None
        data = {"item_id": item_id, "amount": amount}
        publish_event("events_stock", "AddStock", data)
        ###await for ack in queue to return response to user (200 or 400)
        start_subscriber('events_orchestrator', process_event, item_id)
        ack_data = retrieve_ack(item_id)
        if ack_data.get('type') == 'AddStock' and ack_data['data'].get('status') == 'succeed':
            return Response(f"Item: {item_id} stock is updated", status=200)
        else:
            return abort(400, DB_ERROR_STR)
    except TimeoutError:
        return abort(400, REQ_ERROR_STR)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)


@app.post('/stock/subtract/<item_id>/<amount>')  ###transfer to orchestrator
def remove_stock(item_id: str, amount: int):
    try:
        ack_data = None
        data = {"item_id": item_id, "amount": amount}
        publish_event("events_stock", "RemoveStock", data)
        start_subscriber('events_orchestrator', process_event, item_id)
        ack_data = retrieve_ack(item_id)
        if ack_data.get('type') == 'RemoveStock' and ack_data['data'].get('status') == 'succeed':
            return Response(f"Item: {item_id} stock is updated", status=200)
        else:
            return abort(400, DB_ERROR_STR)
    except TimeoutError:
        return abort(400, REQ_ERROR_STR)
        ###await for ack in queue to return response to user (200 or 400)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)

@app.get('/stock/find/<item_id>')
def find_item(item_id: str):
    try:
        data = {"item_id": item_id}
        publish_event("events_stock", "FindItem", data)
        start_subscriber('events_orchestrator', process_event, item_id)
        ack_data = retrieve_ack(item_id)
        if ack_data.get('type') == 'FindItem' and ack_data['data'].get('status') == 'succeed':
            logger.info(f"Item: {item_id} found")
            return jsonify(
                {
                    "stock": ack_data['data']['stock'],
                    "price": ack_data['data']['price']
                }
            )
        else:
            logger.error(f"Item: {item_id} not found")
            abort(400, DB_ERROR_STR)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)


@app.post('/stock/batch_init/<n>/<starting_stock>/<item_price>')
def batch_init_users_stock(n: int, starting_stock: int, item_price: int):
    try:

        ack_data = None
        data = {"n": int(n), "starting_stock": int(starting_stock), "item_price": int(item_price)}
        publish_event("events_stock", "BatchInit", data)
        start_subscriber('events_orchestrator', process_event, "BatchInitStock")
        ack_data = retrieve_ack("BatchInitStock")
        if ack_data.get('type') == 'BatchInit' and ack_data['data'].get('status') == 'succeed':
            logger.info("Batch init for stock successful")
            return Response("Batch init successful", status=200)
        else:
            return abort(400, DB_ERROR_STR)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)


# ------------------------------------------------------------------------------------------------------------------


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
