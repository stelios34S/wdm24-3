import logging
import os
import atexit
import random
import json
import uuid
import redis
import requests

from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response, g

from rabbitmq_utils import publish_event, start_subscriber

DB_ERROR_STR = "DB error"
REQ_ERROR_STR = "Requests error"
GATEWAY_URL = os.environ["GATEWAY_URL"]

app = Flask("order-service")
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

logging.getLogger("pika").setLevel(logging.WARNING)
logger.info(f'{GATEWAY_URL} gateway url')
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


class OrderValue(Struct):
    paid: bool
    items: list[tuple[str, int]]
    user_id: str
    total_cost: int




def get_order_from_db(order_id: str) -> OrderValue | None: ###Does not need to get transferred
    try:
        # get serialized data
        db=get_db()
        entry: bytes = db.get(order_id)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    # deserialize data if it exists else return null
    entry: OrderValue | None = msgpack.decode(entry, type=OrderValue) if entry else None
    if entry is None:
        # if order does not exist in the database; abort
        abort(400, f"Order: {order_id} not found!")
    return entry


# @app.post('/create/<user_id>')
def create_order(data): #### ENDPOINT TRANSFERRED TO ORCHESTRATOR
    key = data['order_id']
    user_id = data['user_id']

    value = msgpack.encode(OrderValue(paid=False, items=[], user_id=user_id, total_cost=0))
    try:
        db = get_db()
        db.set(key, value)
        logger.info(f"Order created: {key}, for user {user_id}")
        publish_event('events_orchestrator', 'CreateOrder', {'correlation_id': key, 'status': 'succeed'})
    except redis.exceptions.RedisError:
        publish_event('events_orchestrator', 'CreateOrder', {'correlation_id': key, 'status': 'failed'})
        abort(400, DB_ERROR_STR)



#@app.post('/batch_init/<n>/<n_items>/<n_users>/<item_price>') ###Does not need to get transferred
def batch_init_users(data):
    n = data["n"]
    n_items = data['n_items']
    n_users = data['n_users']
    item_price = data['item_price']

    # logger.info(f"Batch init  n: {n}, n_items: {n_items}, n_users: {n_users}, item_price: {item_price}")
    def generate_entry() -> OrderValue:
        user_id = random.randint(0, n_users - 1)
        item1_id = random.randint(0, n_items - 1)
        item2_id = random.randint(0, n_items - 1)
        value = OrderValue(paid=False,
                           items=[(f"{item1_id}", 1), (f"{item2_id}", 1)],
                           user_id=f"{user_id}",
                           total_cost=2 * item_price)
        return value

    kv_pairs: dict[str, bytes] = {f"{i}": msgpack.encode(generate_entry())
                                  for i in range(n)}
    try:
        db = get_db()
        db.mset(kv_pairs)
        logger.info("Batch init for orders successful")
        publish_event('events_orchestrator', 'BatchInit', {'status': 'succeed',"correlation_id": "BatchInitOrders"})
    except redis.exceptions.RedisError:
        publish_event('events_orchestrator', 'BatchInit', {'status': 'failed', "correlation_id": "BatchInitOrders"})
        abort(400, DB_ERROR_STR)


#@app.get('/find/<order_id>') ###Does not need to get transferred
def find_order(data):
    order_id = data['order_id']
    try:
        order_entry: OrderValue = get_order_from_db(order_id)
        paid = order_entry.paid
        items = order_entry.items
        user_id = order_entry.user_id
        total_cost = order_entry.total_cost
        publish_event('events_orchestrator', 'FindOrder', {'correlation_id': order_id, 'status': 'succeed', 'paid': paid, 'items': items, 'user_id': user_id, 'total_cost': total_cost})
    except redis.exceptions.RedisError:
        publish_event('events_orchestrator', 'FindOrder', {'correlation_id': order_id, 'status': 'failed'})
        abort(400, DB_ERROR_STR)



def send_post_request(url: str):
    try:
        response = requests.post(url)
    except requests.exceptions.RequestException:
        abort(400, REQ_ERROR_STR)
    else:
        return response


def send_get_request(url: str):
    try:
        response = requests.get(url)
    except requests.exceptions.RequestException:
        abort(400, REQ_ERROR_STR)
    else:
        return response


# @app.post('/addItem/<order_id>/<item_id>/<quantity>')
def add_item(data): #### ENDPOINT TRANSFERRED TO ORCHESTRATOR
    order_id = data['order_id']
    item_id = data['item_id']
    quantity = data['quantity']
    if data['price']:
        price = data['price']
    else:
        price = 0
    status = data['status']
    order_entry: OrderValue = get_order_from_db(order_id)
    #start_subscriber_oneoff('events_order', process_event, item_id)
    #item_reply = send_get_request(f"{GATEWAY_URL}/stock/find/{item_id}")
    if status == 'failed':
        # Request failed because item does not I
        publish_event('events_orchestrator', 'ItemAdded', {'correlation_id': order_id, 'status': 'failed'})
        abort(400, f"Item: {item_id} does not exist!")
    order_entry.items.append((item_id, int(quantity)))
    order_entry.total_cost += int(quantity) * price
    try:
        db = get_db()
        db.set(order_id, msgpack.encode(order_entry))
        logger.info(f"Item: {item_id} added to: {order_id} price updated to: {order_entry.total_cost}")
        publish_event('events_orchestrator', 'ItemAdded', {'correlation_id': order_id, 'status': 'succeed','total_cost': order_entry.total_cost})
    except redis.exceptions.RedisError:
        publish_event('events_orchestrator', 'ItemAdded', {'correlation_id': order_id, 'status': 'failed'})
        abort(400, REQ_ERROR_STR)

def handle_add_item_check(data):
    order_id = data['order_id']
    item_id = data['item_id']
    quantity = data['quantity']
    order_entry: OrderValue = get_order_from_db(order_id)
    publish_event('events_stock', 'AddItemCheck', {"item_id": item_id, "order_id": order_id, "quantity": quantity})


#####STILL UNSURE ABOUT THIS METHOF
def rollback_stock(removed_items: list[tuple[str, int]]):
    for item_id, quantity in removed_items:
        send_post_request(f"{GATEWAY_URL}/stock/add/{item_id}/{quantity}")


# @app.post('/checkout/<order_id>')
def checkout(data): #### ENDPOINT TRANSFERRED TO ORCHESTRATOR
    order_id = data['order_id']
    try:
        order_entry: OrderValue = get_order_from_db(order_id)
        publish_event("events_payment", "ProcessPayment", {
            'order_id': order_id,
            'user_id': order_entry.user_id,
            'total_cost': order_entry.total_cost,
            'items': order_entry.items,
        })
    except redis.exceptions.RedisError:
        publish_event('events_orchestrator', 'Checkout', {'correlation_id': order_id, 'status': 'failed'})
        abort(400, DB_ERROR_STR)
    #send_post_request_orch(f"{GATEWAY_URL}/acks", json.dumps({'type': 'Checkout', 'status': 'initiated'}))


def handle_issue_refund(data):
    order_id = data['order_id']
    publish_event('events_orchestrator', 'IssueRefund', {'correlation_id': order_id, 'status': 'succeed'})

def handle_payment_successful(data):
    order_id = data['order_id']
    logger.info(f"Received payment, data: {data}")
    order_entry: OrderValue = get_order_from_db(order_id)
    order_entry.paid = True
    try:
        db = get_db()
        db.set(order_id, msgpack.encode(order_entry))
        logger.info(f"Payment successful for order {order_id}")
    except redis.exceptions.RedisError:
        abort(400, DB_ERROR_STR)


def handle_payment_failed(data):
    order_id = data['order_id']
    order_entry: OrderValue = get_order_from_db(order_id)
    order_entry.paid = False
    try:
        db = get_db()
        db.set(order_id, msgpack.encode(order_entry))
        logger.info(f"Payment failed for order {order_id}")
        publish_event('events_orchestrator', 'Checkout', {'correlation_id': order_id, 'status': 'failed'})
    except redis.exceptions.RedisError:
        abort(400, DB_ERROR_STR)


def handle_stock_reserved(data):
    order_id = data['order_id']
    logger.debug(f"Checkout successful for order {order_id}")
    publish_event('events_orchestrator', 'Checkout', {'correlation_id': order_id, 'status': 'succeed'})



def handle_stock_failed(data):
    order_id = data['order_id']
    order_entry: OrderValue = get_order_from_db(order_id)
    publish_event('events_payment', 'IssueRefund', {
        'order_id': order_id,
        'user_id': order_entry.user_id,
        'total_cost': order_entry.total_cost
    })
    #send_post_request_orch(f"{GATEWAY_URL}/acks", json.dumps({'type': 'Stock', 'status': 'failed'})) ###???? maybe we dont need this
    logger.info(f"Stock failed for order: {order_id}")



def process_event(ch, method, properties, body):
    with app.app_context():
        event = json.loads(body)
        event_type = event['type']
        data = event['data']
        if event_type == 'OrderCreation':
            create_order(data)
        if event_type == 'AddItem':
            add_item(data)
        if event_type == 'Checkout':
            checkout(data)
        if event_type == 'RefundIssued':
            handle_issue_refund(data)
        if event_type == 'PaymentSuccessfulOrder':
            handle_payment_successful(data)
        elif event_type == 'PaymentFailed':
            handle_payment_failed(data)
        elif event_type == 'StockReserved':
            handle_stock_reserved(data)
        elif event_type == 'StockFailed':
            handle_stock_failed(data)
        elif event_type == 'AddItemCheck':
            handle_add_item_check(data)
        elif event_type == 'BatchInit':
            batch_init_users(data)
        elif event_type == 'FindOrder':
            find_order(data)




start_subscriber('events_order', process_event)

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
