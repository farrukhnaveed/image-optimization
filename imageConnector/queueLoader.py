import pika
import json
import sys
import os
import requests # request img from web
from dotenv import load_dotenv
load_dotenv()

EXCHANGE = os.getenv('RABBITMQ_EXCHANGE')
QUEUE_NAME = os.getenv('RABBITMQ_QUEUE')
HOST = os.getenv('RABBITMQ_HOST')
PORT = os.getenv('RABBITMQ_PORT')
USERNAME = os.getenv('RABBITMQ_USERNAME')
PASSWORD = os.getenv('RABBITMQ_PASSWORD')
THRESHOLD = int(os.getenv('RABBITMQ_QUEUE_THRESHOLD'))
MESSAGE_SIZE = int(os.getenv('RABBITMQ_MESSAGE_SIZE'))

IDS_FILE_PATH = '../lastIdPushed.txt'
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


def getLastPushedId():
    f = open(IDS_FILE_PATH, "w")
    f.write(str(0))
    f.close()
    if os.path.isfile(IDS_FILE_PATH):

        f = open(IDS_FILE_PATH, "r")
        id = f.readline()
        if id == "":
            return 0
        else:
            return int(id)
    else:
        return 0


def setLastPushedId(newLastId):
    try:
        f = open(IDS_FILE_PATH, "w")
        f.write(str(newLastId))
        f.close()
    except Exception as e:
        raise
    else:
        pass
    finally:
        pass

def getPropertyImagesParamsDB():
    lastPushedId = getLastPushedId()
    newLastId = lastPushedId + THRESHOLD
    setLastPushedId(newLastId)
    # imageIds = ",".join([str(k) for k in range(lastPushedId + 1, newLastId + 1)])
    messages = []
    tempIds = []
    count = 0
    for k in range(lastPushedId + 1, newLastId + 1):
        tempIds.append(str(k))
        count = count + 1

        if count >= MESSAGE_SIZE:
            count = 0
            messages.append({
                "source": "db",
                "table": "BKP_fc_rental_photos",
                "columns": "product_image_dir,product_image",
                "imageIds": ",".join(tempIds),
                "urlPattern": "https://img.cuddlynest.com/images/listings/{}{}",
            })
            tempIds = []

    if (len(tempIds) > 0):
        messages.append({
            "source": "db",
            "table": "BKP_fc_rental_photos",
            "columns": "product_image_dir,product_image",
            "imageIds": ",".join(tempIds),
            "urlPattern": "https://img.cuddlynest.com/images/listings/{}{}",
        })
    return messages


# python callable function
def push_image_ids():
    queueCount = getQueueCount()

    if queueCount < THRESHOLD:
        messages = getPropertyImagesParamsDB()
        credentials = pika.PlainCredentials(USERNAME, PASSWORD)
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(heartbeat=600, blocked_connection_timeout=300, host=HOST, port=PORT, credentials=credentials)
        )
        channel = connection.channel()
        channel.exchange_declare(EXCHANGE, durable=True, exchange_type="direct")
        channel.queue_declare(queue=QUEUE_NAME, durable=True)
        channel.queue_bind(exchange=EXCHANGE, queue=QUEUE_NAME, routing_key=QUEUE_NAME)

        for message in messages:
            channel.basic_publish(
                exchange=EXCHANGE,
                routing_key=QUEUE_NAME,
                body=json.dumps(message),
                properties=pika.BasicProperties(
                    delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE
                ),
            )
        connection.close()
        return " [x] Pushed %r" % str(messages)
    else:
        return " [x] Not pushed, Queue count: %r" % str(queueCount)


if __name__ == "__main__":
    result  = push_image_ids()
    print(result)