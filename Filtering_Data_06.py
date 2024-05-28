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
from pyspark.sql import functions as F


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


    # Task 6 - Filtering Data - (1) Trip counts greater than 0 and less than 1000 for different Pickup Borough at different time of day

    # Grouping by 'Pickup_Borough' and 'time_of_day', then counting the number of trips
    trip_counts_df = final_df.groupBy("Pickup_Borough", "time_of_day").count().alias("trip_count")
    
    # Filtering for trip counts greater than 0 and less than 1000
    filtered_trip_counts_df = trip_counts_df.filter((F.col("count") > 0) & (F.col("count") < 1000))
    
    # Rename the 'count' column to 'trip_count'
    filtered_trip_counts_df = filtered_trip_counts_df.withColumnRenamed("count", "trip_count")
    
    # Showing the results
    filtered_trip_counts_df.show(truncate=False)

    # Task 6 - Filtering Data - (2) Number of trips for each Pickup Borough in the evening time
    
    # Filtering for trips in the evening
    evening_trips_df = final_df.filter(F.col("time_of_day") == "evening")
    
    # Grouping by 'Pickup_Borough' and counting the number of trips
    evening_trip_counts_df = evening_trips_df.groupBy("Pickup_Borough").count()
    
    # Rename the 'count' column to 'trip_count'
    evening_trip_counts_df = evening_trip_counts_df.withColumnRenamed("count", "trip_count")
    
    # Add the 'time_of_day' column back to the DataFrame for the final output
    evening_trip_counts_df = evening_trip_counts_df.withColumn("time_of_day", F.lit("evening"))
    
    # Selecting the required columns and showing the results
    final_output_df = evening_trip_counts_df.select("Pickup_Borough", "time_of_day", "trip_count")
    
    # Showing the results
    final_output_df.show(truncate=False)

    
    # Task 6 - Filtering Data - (3) Number of trips that started in Brooklyn and ended in Staten Island
    
    # Filtering for trips that start in Brooklyn and end in Staten Island
    brooklyn_to_staten_island_df = final_df.filter(
        (F.col("Pickup_Borough") == "Brooklyn") & 
        (F.col("Dropoff_Borough") == "Staten Island")
    )
    
    # Counting the number of trips
    num_trips = brooklyn_to_staten_island_df.count()
    
    brooklyn_to_staten_island_samples = brooklyn_to_staten_island_df.select(
        "Pickup_Borough", 
        "Dropoff_Borough", 
        "Pickup_Zone"
    )
    
    # Showing 10 samples
    brooklyn_to_staten_island_samples.show(10, truncate=False)
    
    # Output the total count of trips to the terminal or write it in a report
    print(f"Total number of trips from Brooklyn to Staten Island: {num_trips}")
    
    
    spark.stop()