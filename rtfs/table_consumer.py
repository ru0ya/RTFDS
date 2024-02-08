#!/usr/bin/python3
"""kafka Consumer"""
from kafka import KafkaConsumer
from json import loads


import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 pyspark-shell'

my_consumer = KafkaConsumer(
        bootstrap_servers=['localhost: 9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='my-group',
        value_deserializer=lambda x: loads(x.decode('utf-8'))
        )


topics_to_subscribe = ["first_top", "second_top", "third_top"]
my_consumer.subscribe(topics_to_subscribe)


try:
    while True:
        for msg in my_consumer:
            if msg.error():
                print("Consumer error: {}".format(msg.error()))
            else:
                print("Recieved message from topic {}: {}".format(
                                msg.topic(),
                                msg.value().decode('utf-8')
                                    )
                                )

        my_consumer.flush()

except KeyboardInterrupt:
    print("Consumer closed")
finally:
    my_consumer.close()
