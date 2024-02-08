#!/usr/bin/python3
"""Creating a topic in kafka"""
import time
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError


admin_client = KafkaAdminClient(
        bootstrap_servers="localhost:9092",
        client_id='actions'
        )

# topic_names = ["first_top", "second_top", "third_top"]


def create_topic(topic_name):
    """
    Function that creates a topic in python kafka

    Args: message - data recieved from producer to
                    generate topic

    Returns: Kafka Topics
    """
    # topic_list = []

    # for topic in topic_names:
    try:
        admin_client.create_topics(
                new_topics=[NewTopic(
                name=topic_name,
                num_partitions=1,
                replication_factor=1
                )],
                validate_only=False
            )
        print(f"Topic '{topic_name}' created successfully!")

    except TopicAlreadyExistsError as e:
        print(f"Topic '{topic_name}' already exists")

    except Exception as e:
        print("An error occured", e)


def recieve_and_create_topics():
    received_messages = ["topic1", "topic2", "third_top"]

    for message in received_messages:
        timestamp = int(time.time())
        topic_names = f"topic_{timestamp}"
        create_topic(topic_names)


if __name__ == "__main__":
    recieve_and_create_topics()
    # while True:
        # create_topic(topic_names)

        # except Exception as e:
          #  print("Error during topic creation:", e)
           # print("Retrying..")
