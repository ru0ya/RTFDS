#!/usr/bin/python3
from pykafka import KafkaClient


# create a kafka client
client = KafkaClient(hosts="localhost:9092")


# Get topic
topic = client.topics['testie']

# create a producer
producer = topic.get_sync_producer()

# send message to topic
producer.produce(b"Hello Kafka! Xoming to you live from Python Kafka")
