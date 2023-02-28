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
QUEUE_NAME = os.getenv('RABBITMQ_DB_QUEUE')
HOST = os.getenv('RABBITMQ_HOST')
PORT = os.getenv('RABBITMQ_PORT')
USERNAME = os.getenv('RABBITMQ_USERNAME')
PASSWORD = os.getenv('RABBITMQ_PASSWORD')

# initializing the default arguments
default_args = {
    "owner": "Farrukh",
    "start_date": datetime(2022, 3, 4),
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

# Instantiate a DAG object
dag_didatravel_properties_images = DAG(
    "dag_didatravel_properties_images",
    default_args=default_args,
    description="fc_product image processing DAG",
    schedule_interval="* * * 1 *",
    catchup=False,
    tags=["image, enhance"],
)

# python callable function
def load_db_queue():
    credentials = pika.PlainCredentials(USERNAME, PASSWORD)
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=HOST, port=PORT, credentials=credentials)
    )
    channel = connection.channel()
    channel.exchange_declare(EXCHANGE, durable=True, exchange_type="direct")
    channel.queue_declare(queue=QUEUE_NAME, durable=True)
    channel.queue_bind(exchange=EXCHANGE, queue=QUEUE_NAME, routing_key=QUEUE_NAME)
    message = {
        "source": "didatravel_properties_images"
    }
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


# Creating first task
start_task = DummyOperator(task_id="start_task", dag=dag_didatravel_properties_images)

producer_task = PythonOperator(
    task_id="load_db_queue_task", python_callable=load_db_queue, dag=dag_didatravel_properties_images
)

# Creating last task
end_task = DummyOperator(task_id="end_task", dag=dag_didatravel_properties_images)

# Set the order of execution of tasks.
start_task >> producer_task >> end_task

