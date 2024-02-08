#!/usr/bin/python3
"""
writes data to postgresql database using pyspark
"""
from pyspark.sql import SparkSession


import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 pyspark-shell'

# create sparksession
spark = SparkSession.builder \
        .appName("WritetoDb") \
        .getOrCreate()

# read data from Postgresql table
postgres_df = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/dummy") \
        .option("dbtable", "cardTrans") \
        .option("user", "postgres") \
        .option("password", "password") \
        .load()

# write data to postgresql table
postgres_df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/dummy") \
        .option("dbtable", "cardTrans") \
        .option("user", "postgres") \
        .option("password", "password") \
        .mode("append") \
        .save()
