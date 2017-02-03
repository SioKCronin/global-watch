#!/bin/bash

spark-submit \
    --master spark://ip-172-31-2-11:7077 \
    --executor-memory 6G \
    --driver-memory 6G \
    --packages TargetHolding/pyspark-cassandra:0.3.5 \
    --conf spark.cassandra.connection.host=172.31.2.11,172.31.2.14,172.31.2.9,172.31.2.7 \
    aggregator.py

#make sure to do the command chmod +x run.sh
