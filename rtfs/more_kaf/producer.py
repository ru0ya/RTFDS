#!/usr/bin/python3
"""kafka producer"""
import time
import json
import random
from datetime import datetime
from data_gen import generate_message
from kafka import KafkaProducer


# serialize messages as json
def serializer(message):
    return json.dumps(message).encode('utf-8')


# kafka producer
producer = KafkaProducer(
        bootstrap_servers = ['localhost: 9092'],
        value_serializer = serializer
        )


if __name__ == '__main__':
    while True:
        # generates message
        dummy_message = generate_message()

        # send it to 'messages' topic
        print(f'Producing message @ {datetime.now()} | \
                Message = {str(dummy_message)}')
        producer.send('messages', dummy_message)

        # sleep for random no of seconds
        time_to_sleep = random.randint(1, 11)
        time.sleep(time_to_sleep)

