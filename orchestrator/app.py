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
def print_ack_queues():
    logger.info("Current state of ack_queues:")
    for correlation_id, queue in ack_queues.items():
        logger.info(f"Queue for correlation ID {correlation_id}:")
        items = []
        try:
            while True:
                item = queue.get_nowait()
                items.append(item)
        except Empty:
            pass
        for item in items:
            logger.info(f"{item}")
            queue.put(item)  # Put items back into the queue after inspecting
    logger.info("End of ack_queues state")

# ----------------------------------ORCHESTRATOR-------------------------------------------------------------
def get_ack_queue(correlation_id):
    if correlation_id not in ack_queues:
        ack_queues[correlation_id] = Queue()
        logger.info(f"Created new queue for correlation ID: {correlation_id}")
    return ack_queues[correlation_id]


@app.route('/acks', methods=['POST'])
def ack_endpoint():
    try:
        ack_data = json.loads(request.json)
        logger.info(f"Received ack: {ack_data}")
        correlation_id = ack_data['correlation_id']
        ack_queue = get_ack_queue(correlation_id)
        ack_queue.put(ack_data)
        #print_ack_queues()
        return jsonify({'status': 'ACK received'}), 200
    except Exception as e:
        logger.error(f"Failed to process ack: {e}")
        return jsonify({'status': 'Failed to process ack'}), 400


def await_ack(correlation_id, timeout=ACK_TIMEOUT):
    ack_queue = get_ack_queue(correlation_id)
    try:
        logger.info(f"Waiting for ack with correlation ID: {correlation_id}")
        #print_ack_queues()  # Print queue contents for debugging before waiting
        ack_data = ack_queue.get(timeout=timeout)
        logger.info(f"Received ack from queue for correlation ID {correlation_id}: {ack_data}")
        return ack_data
    except Empty:
        logger.error("ACK timed out")
        raise TimeoutError("ACK timed out")


# -------------------------------------------------------------------------------------------------------------
# ----------------------------------ORDER SERVICE-------------------------------------------------------------
@app.post('/create/<user_id>')
def create_order(user_id: str):
    try:
        key = str(uuid.uuid4())
        data = {"order_id": key, "user_id": user_id}
        publish_event("events_order", "OrderCreation", json.dumps(data))
        # logger.info(f"Order created: {key}, for user {user_id}")
        ###await for ack in queue to return response to user (200 or 400)
        ##create order success or failure
        time.sleep(10)
        ack = await_ack(key)
        if ack.get("type")== "CreateOrder" and ack.get('status') == 'succeeded':
            return Response("Order Created", status=200)
        else:
            return abort(400, DB_ERROR_STR)
    except TimeoutError:
        return abort(400, REQ_ERROR_STR)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)



@app.post('/checkout/<order_id>')
def checkout(order_id: str):
    try:
        logger.debug(f"Checking out {order_id}")
        data = {"order_id": order_id}
        publish_event("events_order", "Checkout", json.dumps(data))
        logger.info("Checkout initiated")
        ack = await_ack(order_id)
        if ack.get('type')== 'Checkout' and ack.get('status') == 'succeeded':
            return Response("Checkout succeeded", status=200)
        if ack.get('type') == 'IssueRefund' and ack.get('status') == 'succeeded':
            return Response("Refund issued", status=200)
        else:
            return abort(400, DB_ERROR_STR)
    except TimeoutError:
        return abort(400, REQ_ERROR_STR)
    except redis.exceptions.RedisError:
    ###await for ack in queue to return response to user (200 or 400) if checkout is successful return 200
    ### or expects an issuerefund message with succeed or failure
        return abort(400, REQ_ERROR_STR)


@app.post('/addItem/<order_id>/<item_id>/<quantity>')
def add_item(order_id: str, item_id: str, quantity: int):
    try:
        data = {"order_id": order_id, "item_id": item_id, "quantity": quantity}
        publish_event("events_order", "AddItem", json.dumps(data))
        ack = await_ack(order_id)
        if ack.get('type') == 'AddItem' and ack.get('status') == 'succeeded':
            return Response("Item added to order", status=200)
        else:
            return abort(400, DB_ERROR_STR)
        ###await for ack in queue to return response to user (200 or 400)
        ###additem message success or failure
    except TimeoutError:
        return abort(400, REQ_ERROR_STR)
    except redis.exceptions.RedisError:
        return abort(400, REQ_ERROR_STR)


# ------------------------------------------------------------------------------------------------------------------
# ----------------------------------PAYMENT SERVICE-------------------------------------------------------------
@app.post('/create_user')
def create_user():
    try:
        key = str(uuid.uuid4())
        data = {"user_id": key}
        publish_event("events_payment", "CreateUser", json.dumps(data))
        ack = await_ack(key)
        if ack.get('type') == 'CreateUser' and ack.get('status') == 'succeeded':
            return Response(f"User: {key} created", status=200)
        else:
            return abort(400, DB_ERROR_STR)
        ###await for ack in queue to return response to user (200 or 400)
        ##create user message success or failure
    except TimeoutError:
        return abort(400, REQ_ERROR_STR)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)


@app.post('/add_funds/<user_id>/<amount>')
def add_credit(user_id: str, amount: int):
    try:
        data = {"user_id": user_id, "amount": amount}
        publish_event("events_payment", "AddCredit", json.dumps(data))
        ack = await_ack(user_id)
        if ack.get('type') == 'AddCredit' and ack.get('status') == 'succeeded':
            return Response(f"User: {user_id} credit is beging updated", status=200)
        else:
            return abort(400, DB_ERROR_STR)
        ###await for ack in queue to return response to user (200 or 400)
        ###addcredit message success or     failure
    except TimeoutError:
        return abort(400, REQ_ERROR_STR)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)


@app.post('/pay/<user_id>/<amount>')
def remove_credit(user_id: str, amount: int):
    logger.info(f"Removing {amount} credit from user: {user_id}")
    try:
        data = {"user_id": user_id, "amount": amount}
        publish_event("events_payment", "RemoveCredit", json.dumps(data))
        ack = await_ack(user_id)
        if ack.get('type') == 'RemoveCredit' and ack.get('status') == 'succeeded':
            return Response(f"User: {user_id} credit is  updated", status=200)
        else:
            return abort(400, DB_ERROR_STR)
        ###await for ack in queue to return response to user (200 or 400)
        ### removecredit message success or failure
    except TimeoutError:
        return abort(400, REQ_ERROR_STR)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)


# ------------------------------------------------------------------------------------------------------------------
# ----------------------------------STOCK SERVICE-------------------------------------------------------------

@app.post('/item/create/<price>')
def create_item(price: int):
    try:
        key = str(uuid.uuid4())
        data = {"price": price, "item_id": key}
        publish_event("events_stock", "CreateItem", json.dumps(data))
        ###await for ack in queue to return response to user (200 or 400)
        ack = await_ack(key)
        if ack.get('type') == 'CreateItem' and ack.get('status') == 'succeeded':
            return Response(f"Item: {key} is created", status=200)
        else:
            return abort(400, DB_ERROR_STR)
    except TimeoutError:
        return abort(400, REQ_ERROR_STR)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)


@app.post('/add/<item_id>/<amount>')  ####Transfered to orchestrator
def add_stock(item_id: str, amount: int):
    try:
        data = {"item_id": item_id, "amount": amount}
        publish_event("events_stock", "AddStock", json.dumps(data))
        ###await for ack in queue to return response to user (200 or 400)
        ack = await_ack(item_id)
        if ack.get('type') == 'AddStock' and ack.get('status') == 'succeeded':
            return Response(f"Item: {item_id} stock is updated", status=200)
        else:
            return abort(400, DB_ERROR_STR)
    except TimeoutError:
        return abort(400, REQ_ERROR_STR)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)



@app.post('/subtract/<item_id>/<amount>')  ###transfer to orchestrator
def remove_stock(item_id: str, amount: int):
    try:
        data = {"item_id": item_id, "amount": amount}
        publish_event("events_stock", "RemoveStock", json.dumps(data))
        ack = await_ack(item_id)
        if ack.get('type') == 'RemoveStock' and ack.get('status') == 'succeeded':
            return Response(f"Item: {item_id} stock is updated", status=200)
        else:
            return abort(400, DB_ERROR_STR)
    except TimeoutError:
        return abort(400, REQ_ERROR_STR)
        ###await for ack in queue to return response to user (200 or 400)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)


# ------------------------------------------------------------------------------------------------------------------
if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
