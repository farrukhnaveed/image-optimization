import pika
import json
import os
import sys
import queueLoader
from dotenv import load_dotenv
sys.path.append('../')
load_dotenv()

EXCHANGE = os.getenv('RABBITMQ_EXCHANGE')
QUEUE_NAME = os.getenv('RABBITMQ_DB_QUEUE')
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
    print(" [x] Received %r" % str(body))
    if data["source"] == "rabbitmq":
        queueLoader.push_image_ids()
    elif data["source"] == "fc_rental_photos":
        queueLoader.set_queue_product_images()
    elif data["source"] == "fc_rental_photos_optimized":
        queueLoader.set_queue_product_images_optimized()
    elif data["source"] == "didatravel_properties_images":
        queueLoader.set_queue_didatravel_properties_images()
    else:
        print(" [x] Not implemented yet")
    print(" [x] Done")


channel.basic_qos(prefetch_count=1)
channel.basic_consume(queue=QUEUE_NAME, on_message_callback=callback, auto_ack=False)

channel.start_consuming()