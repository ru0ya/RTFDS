#!/usr/bin/python3
"""
Kafka producer
"""
import os
import time
import random
import psycopg2
from time import sleep
from json import dumps
from kafka import KafkaProducer

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.1..12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 pyspark-shell'

my_producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer= lambda x: dumps(x).encode('utf-8')
        )

conn = psycopg2.connect(
        dbname="dummy",
        user="postgres",
        password="password",
        host="localhost",
        port=5432
        )

cursor = conn.cursor()

def read_table(cursor):
    """
    Function to randomly read data from customer profile table

    Args:
        cursor - connection point

    Returns:
        Random data from table else None
    """
    table = "customer_profile"
    cursor.execute(f"""
    SELECT * FROM {table} ORDER BY RANDOM() LIMIT 5;
    """
    )
    data = cursor.fetchall()

    return data


if __name__ == "__main__":
    topic = 'customerProfile'
    while True:
        read = read_table(cursor)
        my_producer.send(topic, read)

        time.sleep(30)
        my_producer.flush()
