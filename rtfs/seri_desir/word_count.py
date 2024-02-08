#!/usr/bin/python3
"""Conducting word count from kafka using spark"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split


spark = SparkSession \
        .builder \
        .appName("StructuredNetworkWordCount") \
        .getOrCreate()


# creates DataFrame representing the stream of
# input lines from connection to localhost:9999
lines = spark \
        .readStream \
        .format("socket") \
        .option("port", 9999) \
        .load()

# split lines into words
words = lines.select(
        explode(
            split(lines.value, " ")
            ).alias("word")
        )

# generate running word count
wordCounts = words.groupBy("word").count()

# runs the query that prints the running counts to the console
query = wordCounts \
        .writeStream \
        .outputMode("complete") \
        .format("console") \
        .start()


query.awaitTermination()
