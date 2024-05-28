import sys, string
import os
import socket
import time
import operator
import boto3
import json
from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.functions import from_unixtime, dayofmonth, date_format, to_date, count, avg, rank, concat_ws, col, month, year, sum as _sum
from graphframes import *
from pyspark.sql.window import Window
from pyspark.sql.functions import desc


if __name__ == "__main__":

    spark = SparkSession\
        .builder\
        .appName("TestDataset")\
        .getOrCreate()
    
    # shared read-only object bucket containing datasets
    s3_data_repository_bucket = os.environ['DATA_REPOSITORY_BUCKET']

    s3_endpoint_url = os.environ['S3_ENDPOINT_URL']+':'+os.environ['BUCKET_PORT']
    s3_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
    s3_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
    s3_bucket = os.environ['BUCKET_NAME']

    hadoopConf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoopConf.set("fs.s3a.endpoint", s3_endpoint_url)
    hadoopConf.set("fs.s3a.access.key", s3_access_key_id)
    hadoopConf.set("fs.s3a.secret.key", s3_secret_access_key)
    hadoopConf.set("fs.s3a.path.style.access", "true")
    hadoopConf.set("fs.s3a.connection.ssl.enabled", "false")

    rideshare_data_path = f"s3a://{s3_data_repository_bucket}/ECS765/rideshare_2023/rideshare_data.csv"
    taxi_zone_lookup_path = f"s3a://{s3_data_repository_bucket}/ECS765/rideshare_2023/taxi_zone_lookup.csv"
    rideshare_data_df = spark.read.option("header", "true").csv(rideshare_data_path)
    taxi_zone_lookup_df = spark.read.option("header", "true").csv(taxi_zone_lookup_path)

    # Join on pickup location
    rideshare_with_pickup = rideshare_data_df.join(
        taxi_zone_lookup_df.withColumnRenamed('LocationID', 'pickup_LocationID'),
        col("pickup_location") == col("pickup_LocationID"),
        "left"
    )
    
    # Drop the duplicate LocationID column and rename columns
    rideshare_with_pickup = rideshare_with_pickup.drop('pickup_LocationID')\
        .withColumnRenamed("Borough", "Pickup_Borough")\
        .withColumnRenamed("Zone", "Pickup_Zone")\
        .withColumnRenamed("service_zone", "Pickup_service_zone")
    
    # Join on dropoff location
    final_df = rideshare_with_pickup.join(
        taxi_zone_lookup_df.withColumnRenamed('LocationID', 'dropoff_LocationID'),
        col("dropoff_location") == col("dropoff_LocationID"),
        "left"
    )
    
    # Drop the duplicate LocationID column and rename columns
    final_df = final_df.drop('dropoff_LocationID')\
        .withColumnRenamed("Borough", "Dropoff_Borough")\
        .withColumnRenamed("Zone", "Dropoff_Zone")\
        .withColumnRenamed("service_zone", "Dropoff_service_zone")
    
    # Convert UNIX timestamp to date format
    final_df = final_df.withColumn("date", from_unixtime(col("date"), "yyyy-MM-dd"))

    # Task 5 - Finding anomalies

    # Filtering for January data
    january_data = final_df.filter(month(col("date")) == 1)
    
    # Converting the 'request_to_pickup' field to integer
    january_data = january_data.withColumn("request_to_pickup", col("request_to_pickup").cast("integer"))
    
    # Calculating the average 'request_to_pickup' for each day in January
    average_waiting_time_by_day = january_data.groupBy(dayofmonth(col("date")).alias("day"))\
        .agg(_sum("request_to_pickup").alias("total_waiting_time"), count("request_to_pickup").alias("num_rides"))\
        .withColumn("average_waiting_time", col("total_waiting_time") / col("num_rides"))\
        .select("day", "average_waiting_time")\
        .orderBy("day")
    
    # Showing the result
    average_waiting_time_by_day.show(31)
    
    # Identifying the day(s) where average waiting time exceeds 300 seconds
    days_with_long_wait = average_waiting_time_by_day.filter(col("average_waiting_time") > 300)
    
    # Showing the result
    days_with_long_wait.show()

    average_waiting_time_by_day.coalesce(1).write.mode("overwrite").options(header=True).csv(f"s3a://{s3_bucket}/Finding_anomalies_csv_output_05/")
    
    spark.stop()