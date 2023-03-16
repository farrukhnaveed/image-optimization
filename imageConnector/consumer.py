import pika
import json
import os
import sys
from db import updateQueue, log, get_queue
from dotenv import load_dotenv
sys.path.append('../')
from process import process
load_dotenv()
from datetime import datetime


EXCHANGE = os.getenv('RABBITMQ_EXCHANGE')
QUEUE_NAME = os.getenv('RABBITMQ_QUEUE')
HOST = os.getenv('RABBITMQ_HOST')
PORT = os.getenv('RABBITMQ_PORT')
USERNAME = os.getenv('RABBITMQ_USERNAME')
PASSWORD = os.getenv('RABBITMQ_PASSWORD')

credentials = pika.PlainCredentials(USERNAME, PASSWORD)
connection= pika.BlockingConnection(pika.ConnectionParameters(heartbeat=600, blocked_connection_timeout=300, host=HOST, port=PORT, credentials= credentials))
channel= connection.channel()
channel.exchange_declare(EXCHANGE, durable=True, exchange_type='direct')

# channel.queue_declare(queue=QUEUE_NAME, durable=True)
print(' [*] Waiting for messages. To exit press CTRL+C')


def callback(ch, method, properties, body):
    ch.basic_ack(delivery_tag=method.delivery_tag)
    data = json.loads(body)
    now = datetime.now()
    dt_string = now.strftime("%d/%m/%Y %H:%M:%S")

    print(" [x] At {}, Received {}".format(dt_string, str(body)))
    updateQueue(data['queue_id'], {
        "status": "running",
        "message": 'started request' 
    })
    
    log(data['queue_id'], 'getting queue object', 'consumer')
    queue_obj = get_queue(data['queue_id'])
    if (queue_obj["image"] == '' or queue_obj["destination"] == ''):
        updateQueue(queue_obj['id'] ,{
            "status": "rejected",
            "message": 'invalid queue data'})
        log(queue_obj['id'], 'invalid message', 'consumer', 'queueObj', 'invalid queue data')
        return
    log(data['queue_id'], 'starting queue processing', 'consumer')
    process(queue_obj)
    log(data['queue_id'], 'finished queue processing', 'consumer')
    print(" [x] Done")


channel.basic_qos(prefetch_count=1)
channel.basic_consume(queue=QUEUE_NAME, on_message_callback=callback, auto_ack=False)

channel.start_consuming()