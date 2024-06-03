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
from flask import Flask, jsonify, abort, Response, request
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


ack_queues = {}


# ----------------------------------ORCHESTRATOR-------------------------------------------------------------
def get_ack_queue(correlation_id):
    if correlation_id not in ack_queues:
        ack_queues[correlation_id] = Queue()
    return ack_queues[correlation_id]


@app.route('/acks', methods=['POST'])
def ack_endpoint():
    try:
        ack_data = request.json
        logger.info(f"Received ack: {ack_data}")
        correlation_id = ack_data.get('correlation_id')
        if correlation_id and correlation_id in ack_queues:
            ack_queues[correlation_id].put(ack_data)
        logger.info(f"Received ack: {ack_data}")
        return jsonify({'status': 'ACK received'}), 200
    except Exception as e:
        logger.error(f"Failed to process ack: {e}")
        return jsonify({'status': 'Failed to process ack'}), 400


def await_ack(correlation_id, timeout=ACK_TIMEOUT):
    ack_queue = get_ack_queue(correlation_id)
    try:
        ack_data = ack_queue.get(timeout=timeout)
        return ack_data
    except not Queue.not_empty:
        raise TimeoutError("ACK timed out")


# -------------------------------------------------------------------------------------------------------------
# ----------------------------------ORDER SERVICE-------------------------------------------------------------
@app.post('/create/<user_id>')
def create_order(user_id: str):
    try:
        publish_event("events", "OrderCreation", json.dumps(user_id))
        # logger.info(f"Order created: {key}, for user {user_id}")
        ###await for ack in queue to return response to user (200 or 400)
        ##create order success or failure
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response("Order Created", status=200)


@app.post('/checkout/<order_id>')
def checkout(order_id: str):
    logger.debug(f"Checking out {order_id}")
    publish_event("events", "Checkout", json.dumps(order_id))
    logger.info("Checkout initiated")
    ###await for ack in queue to return response to user (200 or 400) if checkout is successful return 200
    ### or expects an issuerefund message with succeed or failure
    return Response("Checkout initiated", status=200)


@app.post('/addItem/<order_id>/<item_id>/<quantity>')
def add_item(order_id: str, item_id: str, quantity: int):
    try:
        data = {"order_id": order_id, "item_id": item_id, "quantity": quantity}
        publish_event("events", "AddItem", json.dumps(data))

        ###await for ack in queue to return response to user (200 or 400)
        ###additem message success or failure
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({"msg": "Item added to order"})


# ------------------------------------------------------------------------------------------------------------------
# ----------------------------------PAYMENT SERVICE-------------------------------------------------------------
@app.post('/create_user')  ####transfer to orchestrator
def create_user():
    try:
        key = str(uuid.uuid4())
        data = {"user_id": key}
        publish_event("events", "CreateUser", json.dumps(data))

        ###await for ack in queue to return response to user (200 or 400)
        ##create user message success or failure
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({'msg': 'User is being created'}), 200


@app.post('/add_funds/<user_id>/<amount>')
def add_credit(user_id: str, amount: int):
    try:
        data = {"user_id": user_id, "amount": amount}
        publish_event("events", "AddCredit", json.dumps(data))

        ###await for ack in queue to return response to user (200 or 400)
        ###addcredit message success or     failure
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"User: {user_id} credit is beging updated", status=200)


@app.post('/pay/<user_id>/<amount>')
def remove_credit(user_id: str, amount: int):
    logger.info(f"Removing {amount} credit from user: {user_id}")
    try:
        data = {"user_id": user_id, "amount": amount}
        publish_event("events", "RemoveCredit", json.dumps(data))

        ###await for ack in queue to return response to user (200 or 400)
        ### removecredit message success or failure
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"User: {user_id} credit is being updated", status=200)


# ------------------------------------------------------------------------------------------------------------------
# ----------------------------------STOCK SERVICE-------------------------------------------------------------

@app.post('/item/create/<price>')
def create_item(price: int):
    try:
        key = str(uuid.uuid4())
        data = {"price": price, "item_id": key}
        publish_event("events", "CreateItem", json.dumps(data))
        ###await for ack in queue to return response to user (200 or 400)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"Item: is being created", status=200)


@app.post('/add/<item_id>/<amount>')  ####Transfered to orchestrator
def add_stock(item_id: str, amount: int):
    try:
        data = {"item_id": item_id, "amount": amount}
        publish_event("events", "AddStock", json.dumps(data))
        ###await for ack in queue to return response to user (200 or 400)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"Item: {item_id} stock is being updated", status=200)


@app.post('/subtract/<item_id>/<amount>')  ###transfer to orchestrator
def remove_stock(item_id: str, amount: int):
    try:
        data = {"item_id": item_id, "amount": amount}
        publish_event("events", "RemoveStock", json.dumps(data))
        ###await for ack in queue to return response to user (200 or 400)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"Item: {item_id} stock is being updated", status=200)


# ------------------------------------------------------------------------------------------------------------------
if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
