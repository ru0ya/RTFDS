#!/usr/bin/python3
"""
Subscribe to kafka topics, analyse data
using spark
"""
import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_json
# import psycopg2

import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 pyspark-shell'


spark = SparkSession.builder \
        .appName("TransAction") \
        .getOrCreate()


kafka_df = spark.readStream\
        .format("kafka")\
        .option("kafka.bootstrap.servers", "kafka:9092")\
        .option("subscribe", "transaction")\
        .load()


# Assumes value is a JSON string
data_df = kafka_df.selectExpr("CAST(value AS STRING) as json_string")


transaction_df = data_df.to_json(
        data_df.select(to_json(data_df.transaction_amount).alias("json_column_name"))
        )

postgres_url = "jdbc:postgresql://localhost:5432/dummy"
postgres_properties = {
        "user": "postgres",
        "password": "password",
        "driver": "org.postgresql.Driver"
        }


query = data_df.writeStream \
        .outputMode("append") \
        .format("jdbc") \
        .option("url", "jdbc:subprotocol:://localhost:5432/dummy") \
        .option("dbtable", "cardTrans")\
        .option("user", "postgres") \
        .option("password", "password") \
        .start()


query.awaitTermination()
