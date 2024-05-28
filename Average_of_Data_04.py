import sys, string
import os
import socket
import time
import operator
import boto3
import json
from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.functions import from_unixtime, date_format, to_date, count, avg, rank, concat_ws, col, month, year, sum as _sum
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

    #print("First few rows of rideshare_data_df:")
    #rideshare_data_df.show()

    #print("First few rows of taxi_zone_lookup_df:")
    #taxi_zone_lookup_df.show()

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
    
    #Task 4 - Average of Data - (1) Average drivers pay during different time of day

    # Calculate the average 'driver_total_pay' during different 'time_of_day' periods
    average_pay_by_time = final_df.groupBy("time_of_day")\
        .agg(avg("driver_total_pay").alias("average_driver_total_pay"))\
        .orderBy(desc("average_driver_total_pay"))
    
    print("Average 'driver_total_pay' during different 'time_of_day' periods: ")
    average_pay_by_time.show()

    # Task 4 - Average of Data - (2) Average trip length during different time of day 

    # Calculate the average 'trip_length' during different 'time_of_day' periods
    average_length_by_time = final_df.groupBy("time_of_day")\
        .agg(avg("trip_length").alias("average_trip_length"))\
        .orderBy(desc("average_trip_length"))

    print("Average 'trip_length' during different time_of_day periods: ")
    average_length_by_time.show()


    # Task 4 - Average of Data - (3) Average earned per mile for each time of day

    # Join the two results above on 'time_of_day'
    average_earnings_per_mile = average_pay_by_time.join(
        average_length_by_time, "time_of_day"
    )
    
    # Calculate average earning per mile
    average_earnings_per_mile = average_earnings_per_mile.withColumn(
        "average_earning_per_mile",
        col("average_driver_total_pay") / col("average_trip_length")
    ).select("time_of_day", "average_earning_per_mile").orderBy(desc("average_earning_per_mile"))

    print("Average earned per mile for each time_of_day period: ")
    average_earnings_per_mile.show()

    
    spark.stop()