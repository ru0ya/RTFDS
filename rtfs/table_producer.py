#!/usr/bin/python3
"""Kafka Producer"""
import time
import psycopg2
from time import sleep
from json import dumps
from kafka import KafkaProducer

import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 pyspark-shell'

my_producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda x: dumps(x).encode('utf-8')
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
    Function to read table

    cursor - connection point

    Returns: table data
    """
    table = "dummy_table"
    cursor.execute(f"""
    SELECT * FROM {table};
    """
    )
    
    data = cursor.fetchall()

    return data


if __name__ == "__main__":
    topic_names = ["first_top", "second_top", "third_top"]
    while True:
        read = read_table(cursor)
        for topic in topic_names:
            my_producer.send('topic', value=read)

        print(f"Data in table: {read}")
        time.sleep(60)
        my_producer.flush()
