#!/usr/bin/python3
"""
Connects to kafka topic and reads
data using pyspark
"""
import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json
from pyspark.sql.types import(
        StructType,
        StructField,
        StringType
        )


import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 pyspark-shell'


# create a SparkSession
spark = SparkSession.builder \
        .appName("ReadFromKafka") \
        .config("spark.serializer",
                "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()

# define schema for Kafka message value
schema = StructType([
    StructField("transaction_id", StringType()),
    StructField("transaction_amount", StringType()),
    StructField("card_no", StringType())
    ])

# read from kafka topic
kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "transaction") \
        .load()

# deserialize JSON data
parsed_df = kafka_df.select(from_json(kafka_df.value.cast("string"),
                                schema).alias("data"))

# print(parsed_df)

# access individual fields
transaction_df = parsed_df.select(
                            "data.transaction_id",
                            "data.transaction_amount",
                            "data.card_no"
                            )

# start streaming query
query = transaction_df.writeStream \
        .outputMode("append") \
        .format("console") \
        .start()

# wait for query to terminate
query.awaitTermination()
