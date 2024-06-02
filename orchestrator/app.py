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

app = Flask("orchestrator")
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

logging.getLogger("pika").setLevel(logging.WARNING)

#----------------------------------IDEA FOR ACKS NEW IDEA-------------------------------------------------------------
#SO WE CAN POSSIBLY CREATE A NEW ENDPOINT IN THE ORCHESTRATOR THAT JUST WAITS FOR ACKS.
# BASICALLY THE OTHER SERIVCES CAN SEND ACKS TO THIS ENDPOINT
#AND WE CAN SHOW THEM TO THE USER.
@app.route('/acks', methods=['POST'])
def ack_endpoint():
    ack_data = request.json
    logger.info(f"Received ack: {ack_data}")
    return jsonify({'data': ack_data}), 200





#----------------------------------ORDER SERVICE-------------------------------------------------------------
@app.post('/create/<user_id>')
def create_order(user_id: str):
    try:
        publish_event("events","OrderCreation",json.dumps(user_id))
        #logger.info(f"Order created: {key}, for user {user_id}")
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response("Order Created",status=200)

@app.post('/checkout/<order_id>')
def checkout(order_id: str):
    logger.debug(f"Checking out {order_id}")
    publish_event("events","Checkout",json.dumps(order_id))
    logger.info("Checkout initiated")
    return Response("Checkout initiated", status=200)
# @app.post('/batch_init/<n>/<n_items>/<n_users>/<item_price>')
# def batch_init_users(n: int, n_items: int, n_users: int, item_price: int):
#     try:
#         data = {"n": int(n), "n_items": int(n_items), "n_users": int(n_users), "item_price": int(item_price)}
#         publish_event("events","BatchInit",json.dumps(data))
#     except redis.exceptions.RedisError:
#         return abort(400, DB_ERROR_STR)
#     return jsonify({"msg": "Batch init for orders successful"})


@app.post('/addItem/<order_id>/<item_id>/<quantity>')
def add_item(order_id: str, item_id: str, quantity: int):
    try:
        data={"order_id": order_id, "item_id": item_id, "quantity": quantity}
        publish_event("events","AddItem",json.dumps(data))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({"msg": "Item added to order"})
#------------------------------------------------------------------------------------------------------------------
#----------------------------------PAYMENT SERVICE-------------------------------------------------------------
@app.post('/create_user') ####transfer to orchestrator
def create_user():
    try:
        publish_event("events","CreateUser","")
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({'msg': 'User is being created'}), 200


@app.post('/add_funds/<user_id>/<amount>')
def add_credit(user_id: str, amount: int):
    try:
        data={"user_id": user_id, "amount": amount}
        publish_event("events","AddCredit",json.dumps(data))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"User: {user_id} credit is beging updated", status=200)

@app.post('/pay/<user_id>/<amount>')
def remove_credit(user_id: str, amount: int):
    logger.info(f"Removing {amount} credit from user: {user_id}")
    try:
        data={"user_id": user_id, "amount": amount}
        publish_event("events","RemoveCredit",json.dumps(data))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"User: {user_id} credit is being updated", status=200)
#------------------------------------------------------------------------------------------------------------------
#----------------------------------STOCK SERVICE-------------------------------------------------------------

@app.post('/item/create/<price>')
def create_item(price: int):
    try:
        data = {"price": price}
        publish_event("events","CreateItem",json.dumps(data))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"Item: is being created", status=200)


@app.post('/add/<item_id>/<amount>') ####Transfered to orchestrator
def add_stock(item_id: str, amount: int):
    try:
        data = {"item_id": item_id, "amount": amount}
        publish_event("events","AddStock",json.dumps(data))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"Item: {item_id} stock is being updated", status=200)

@app.post('/subtract/<item_id>/<amount>') ###transfer to orchestrator
def remove_stock(item_id: str, amount: int):
    try:
        data = {"item_id": item_id, "amount": amount}
        publish_event("events","RemoveStock",json.dumps(data))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"Item: {item_id} stock is being updated", status=200)
#------------------------------------------------------------------------------------------------------------------
if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
