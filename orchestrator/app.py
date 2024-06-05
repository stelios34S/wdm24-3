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




# ----------------------------------ORCHESTRATOR-------------------------------------------------------------

def process_event(body):
    global ack_data

    ack_data = body



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
        start_subscriber('events_orchestrator', process_event, key)
        logger.info(f'message received with ACK{ack_data}')
        ##ack = await_ack(key)
        if ack_data.get("type")== "CreateOrder" and ack_data['data'].get('status') == 'succeed':
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
        start_subscriber('events_orchestrator', process_event, order_id)
        if ack_data.get('type')== 'Checkout' and ack_data['data'].get('status') == 'succeed':
            return Response("Checkout succeed", status=200)
        if ack_data.get('type') == 'IssueRefund' and ack_data['data'].get('status') == 'succeed':
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
        start_subscriber('events_orchestrator', process_event, order_id)
        if ack_data.get('type') == 'AddItem' and ack_data['data'].get('status') == 'succeed':
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
        start_subscriber('events_orchestrator', process_event, key)
        if ack_data.get('type') == 'CreateUser' and ack_data["data"].get('status') == 'succeed':
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
        start_subscriber('events_orchestrator', process_event, user_id)
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


@app.post('/pay/<user_id>/<amount>')
def remove_credit(user_id: str, amount: int):
    logger.info(f"Removing {amount} credit from user: {user_id}")
    try:
        data = {"user_id": user_id, "amount": amount}
        publish_event("events_payment", "RemoveCredit", json.dumps(data))
        start_subscriber('events_orchestrator', process_event, user_id)
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


# ------------------------------------------------------------------------------------------------------------------
# ----------------------------------STOCK SERVICE-------------------------------------------------------------

@app.post('/item/create/<price>')
def create_item(price: int):
    try:
        key = str(uuid.uuid4())
        data = {"price": price, "item_id": key}
        publish_event("events_stock", "CreateItem", json.dumps(data))
        ###await for ack in queue to return response to user (200 or 400)
        start_subscriber('events_orchestrator', process_event, key)
        if ack_data.get('type') == 'CreateItem' and ack_data['data'].get('status') == 'succeed':
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
        start_subscriber('events_orchestrator', process_event, item_id)
        if ack_data.get('type') == 'AddStock' and ack_data['data'].get('status') == 'succeed':
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
        start_subscriber('events_orchestrator', process_event, item_id)
        if ack_data.get('type') == 'RemoveStock' and ack_data['data'].get('status') == 'succeed':
            return Response(f"Item: {item_id} stock is updated", status=200)
        else:
            return abort(400, DB_ERROR_STR)
    except TimeoutError:
        return abort(400, REQ_ERROR_STR)
        ###await for ack in queue to return response to user (200 or 400)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)


# ------------------------------------------------------------------------------------------------------------------
# def process_event(ch, method, properties, body):
#     event = json.loads(body)
#     event_type = event['type']
#     data = event['data']
#
#     if event_type == 'CreateOrder':
#         logger.info(f"Order created: {event}")
#         return event
    # if event_type == 'ItemAdded':
    #     handle_add_item(data)
    # if event_type == 'Checkout':
    #     handle_checkout(data)
    # if event_type == 'IssueRefund':
    #     handle_issue_refund(data)
    # if event_type == 'CreateUser':
    #     handle_create_user(data)
    # elif event_type == 'AddCredit':
    #     handle_add_credit(data)
    # elif event_type == 'RemoveCredit':
    #     handle_remove_credit(data)
    # elif event_type == 'CreateItem':
    #     handle_create_item(data)
    # elif event_type == 'AddStock':
    #     handle_add_stock(data)
    # elif event_type == 'RemoveStock':
    #     handle_remove_stock(data)





if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
