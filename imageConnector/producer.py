import pika
import json
import sys
import os
from dotenv import load_dotenv
load_dotenv()

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
channel.queue_declare(queue= QUEUE_NAME, durable=True)
channel.queue_bind(exchange=EXCHANGE, queue=QUEUE_NAME, routing_key=QUEUE_NAME)

source = sys.argv[1]

if (source == "db"):
    # python producer.py db BKP_fc_rental_photos product_image_dir,product_image,id 1,2,3 https://img.cuddlynest.com/images/listings/{}{}
    message = {
        "source": source,
        "table": sys.argv[2],
        "columns": sys.argv[3],
        "imageIds": sys.argv[4],
        "urlPattern": sys.argv[5]
    }
else:
    message = {}

channel.basic_publish(
    exchange=EXCHANGE,
    routing_key=QUEUE_NAME,
    body=json.dumps(message),
    properties=pika.BasicProperties(
        delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE
    ),
    mandatory=True
)
print(" [x] Sent %r" % str(message))
connection.close()