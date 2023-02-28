import json, os
import pika

# datetime
from datetime import timedelta, datetime

# The DAG object
from airflow import DAG

# Operators
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

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

# initializing the default arguments
default_args = {
    "owner": "Farrukh",
    "start_date": datetime(2022, 3, 4),
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

# Instantiate a DAG object
fc_product_dag = DAG(
    "fc_product_dag",
    default_args=default_args,
    description="Image processing DAG",
    schedule_interval="* * * 1 *",
    catchup=False,
    tags=["image, enhance"],
)


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
            pika.ConnectionParameters(host=HOST, port=PORT, credentials=credentials)
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
        return " [x] Pushed %r" % str(message)
    else:
        return " [x] Not pushed, Queue count: %r" % str(queueCount)


# Creating first task
start_task = DummyOperator(task_id="start_task", dag=fc_product_dag)

producer_task = PythonOperator(
    task_id="push_image_ids_task", python_callable=push_image_ids, dag=fc_product_dag
)

# Creating last task
end_task = DummyOperator(task_id="end_task", dag=fc_product_dag)

# Set the order of execution of tasks.
start_task >> producer_task >> end_task
