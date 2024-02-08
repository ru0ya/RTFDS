#!/usr/bin/python3
from pykafka import KafkaClient


client = KafkaClient(hosts="localhost:9092")


# Get the topic
topic = client.topics['testie']

# create a consumer
consumer = topic.get_simple_consumer()

# iterate through messages in the topic
for message in consumer:
    if message is not None:
        print(message.offset, message.value)

