import pika
import os
from dotenv import load_dotenv
load_dotenv()
from time import sleep
import subprocess
import random

EXCHANGE = os.getenv('RABBITMQ_EXCHANGE')
QUEUE_NAME = os.getenv('RABBITMQ_QUEUE')
HOST = os.getenv('RABBITMQ_HOST')
PORT = os.getenv('RABBITMQ_PORT')
USERNAME = os.getenv('RABBITMQ_USERNAME')
PASSWORD = os.getenv('RABBITMQ_PASSWORD')

def getConsumerCount():
    credentials = pika.PlainCredentials(USERNAME, PASSWORD)
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=HOST, port=PORT, credentials=credentials)
    )
    channel = connection.channel()
    channel.exchange_declare(EXCHANGE, durable=True, exchange_type="direct")
    q = channel.queue_declare(queue=QUEUE_NAME, durable=True)

    q_len = q.method.consumer_count
    return int(q_len)


if __name__ == "__main__":
    while(1):
        # consumer_count = getConsumerCount()
        # print('consumer count: {}'.format(consumer_count))
        # if (consumer_count < 2):
        # os.system("python /app/imageConnector/consumer.py")
        proc = subprocess.Popen(
            # Let it ping more times to run longer.
            ["python", "/app/imageConnector/consumer.py"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        # Popen is non-blocking, and returns immediately. The shell command is run in the background.
        # proc.poll() returns the exit code of the shell command if it is finished, otherwise returns None.
        count = 1
        while proc.poll() is None:
            statement = "Count: {}, PID: {}, Consumer is still running...".format(count, proc.pid)
            count = count + 1
            print(statement)
            sleep(10)

        # When arriving here, the shell command has finished.
        # Check the exit code of the shell command:
        print(proc.poll())
        # sleep(5)
        