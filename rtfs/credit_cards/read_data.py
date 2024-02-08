#!/usr/bin/python3
"""
Reads data from credit card table
and sends it to Kafka
"""
import time
from json import dumps
from kafka import KafkaProducer
import psycopg2
from decimal import Decimal


# connect to postgresql tdatabase
conn = psycopg2.connect(
        dbname="dummy",
        user="postgres",
        password="password",
        host="localhost",
        port=5432
        )

cursor = conn.cursor()


# function to handle serialization of Decimal objects
def decimal_serializer(obj):
    if isinstance(obj, Decimal):
        return str(obj)
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")


# kafka producer configuration
producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda x: dumps(x, default=decimal_serializer).encode('utf-8')
        )


def readTable(cursor):
    """
    Function that reads data from a postgresql table

    Args: cursor- connection point

    Returns: data from table
    """
    table = "cardTrans"
    cursor.execute(f"SELECT * FROM {table}")
    data = cursor.fetchall()

    return data


if __name__ == "__main__":
    while True:
        # read 10 records from table
        records = readTable(cursor)[:10]

        # send records to kafka topic
        for record in records:
            producer.send('transaction', value=record)
            print("sent record:", record)

        # flush message to kafka
        producer.flush()

        # wait for 1 second
        time.sleep(1)
