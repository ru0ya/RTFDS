#!/usr/bin/python3
"""Using kafka and spark"""
import time
import psycopg2
from time import sleep
from json import dumps
from kafka import KafkaProducer
from kafka import KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError


conn = psycopg2.connect(
        dbname="dummy",
        user="postgres",
        password="password",
        host="localhost",
        port=5432
        )

cursor = conn.cursor()


my_producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda x: dumps(x).encode('utf-8')
        )


admin_client = KafkaAdminClient(bootstrap_servers='localhost:9092')
consumer = KafkaConsumer(bootstrap_servers='localhost:9092')


def readTable(cursor):
    """
    Function to read data from table

    Args: cursor - connection point

    Returns: data in table
    """
    table = "cardTrans"
    cursor.execute(f"""
        SELECT * FROM {table}
        """)

    data = cursor.fetchall()

    return data


def createTopic(topic_name):
    """
    creates a topic in kafka

    Returns: topics
    """
    # kafka broker configuration
    broker_config = {
            'bootstrap.servers': 'localhost:9092'
            }

    # KafkaAdminClient instance
    admin_client = KafkaAdminClient(
            bootstrap_servers=broker_config['bootstrap.servers']
            )

    # create NewTopic instance with topic configuration
    """topic_config = NewTopic(
            name='transactions',
            num_partitions=1,
            replication_factor=1
            )"""

    # return admin_client.create_topics(new_topics=[topic_config])

    existing_topics = consumer.topics()
    if topic_name not in existing_topics:
        topic = NewTopic(
                name='transaction',
                num_partitions=1,
                replication_factor=1
                )
        admin_client.create_topics(
                new_topics=[topic],
                validate_only=False
                )


if __name__ == "__main__":
    while True:
        read = readTable(cursor)
        topic_name = "transaction"
        createTopic(topic_name)
        my_producer.send(topic_name, value=read)

        time.sleep(10)
        my_producer.flush()
