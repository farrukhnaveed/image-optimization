import pika
import json
import os
import db
from dotenv import load_dotenv
load_dotenv()

EXCHANGE = os.getenv('RABBITMQ_EXCHANGE')
QUEUE_NAME = os.getenv('RABBITMQ_QUEUE')
HOST = os.getenv('RABBITMQ_HOST')
PORT = os.getenv('RABBITMQ_PORT')
USERNAME = os.getenv('RABBITMQ_USERNAME')
PASSWORD = os.getenv('RABBITMQ_PASSWORD')
THRESHOLD = int(os.getenv('RABBITMQ_QUEUE_THRESHOLD'))

def getQueueCount():
    credentials = pika.PlainCredentials(USERNAME, PASSWORD)
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=HOST, port=PORT, credentials=credentials)
    )
    channel = connection.channel()
    channel.exchange_declare(EXCHANGE, durable=True, exchange_type="direct")
    q = channel.queue_declare(queue=QUEUE_NAME, durable=True)

    q_len = q.method.message_count
    return int(q_len)


# python callable function
def push_image_ids():
    queueCount = getQueueCount()

    if queueCount < THRESHOLD:
        ids = db.getPendingQueueIds(THRESHOLD)
        credentials = pika.PlainCredentials(USERNAME, PASSWORD)
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(heartbeat=600, blocked_connection_timeout=300, host=HOST, port=PORT, credentials=credentials)
        )
        channel = connection.channel()
        channel.exchange_declare(EXCHANGE, durable=True, exchange_type="direct")
        channel.queue_declare(queue=QUEUE_NAME, durable=True)
        channel.confirm_delivery()
        channel.queue_bind(exchange=EXCHANGE, queue=QUEUE_NAME, routing_key=QUEUE_NAME)

        for id in ids:
            channel.basic_publish(
                exchange=EXCHANGE,
                routing_key=QUEUE_NAME,
                body=json.dumps({
                    "queue_id": id
                }),
                properties=pika.BasicProperties(
                    delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE
                ),
                mandatory=True,
            )
        connection.close()
        for id in ids:
            db.updateQueue(id ,{
                "status": "pushed",
                "message": 'pushed to queue'})
            
            db.log(id, 'pushed to queue')
        return " [x] Pushed %r" % str(ids)
    else:
        return " [x] Not pushed, Queue count: %r" % str(queueCount)


def set_queue_product_images():
    source = "fc_rental_photos"
    last_image_id = db.get_last_queued_image_id(source)
    db.add_queue(source, last_image_id, THRESHOLD)

if __name__ == "__main__":
    set_queue_product_images()
    result = push_image_ids()
    print(result)