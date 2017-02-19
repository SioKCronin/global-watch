#!/bin/bash

spark-submit \
    --master spark://ip-***-**-*-**:7077 \
    --executor-memory 6G \
    --driver-memory 6G \
    --packages TargetHolding/pyspark-cassandra:0.3.5,com.databricks:spark-csv_2.10:1.2.0 \
    --conf spark.cassandra.connection.host=***.**.*.*,***.**.*.**,***.**.*.* \
    second.py
