import sys, string
import os
import socket
import time
import operator
import boto3
import json
from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.functions import from_unixtime, date_format, to_date, count, rank, concat_ws, col, month, year, sum as _sum
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
    
    #TASK 3 - Top-K Processing - (1) Top 5 popular pickup boroughs each month

    # Extract the month from the 'date' and cast it as integer
    final_df = final_df.withColumn("Month", month(col("date").cast("date")))

    # Group by 'Pickup_Borough' and 'Month', and count the number of trips
    borough_monthly_trip_counts = final_df.groupBy("Pickup_Borough", "Month").count()

    # Define the window spec for ranking by trip count within each month
    windowSpec = Window.partitionBy("Month").orderBy(desc("count"))

    # Apply the window spec to rank the boroughs by trip count within each month
    top_boroughs_monthly = borough_monthly_trip_counts.withColumn("rank", rank().over(windowSpec)) \
                                                      .filter(col("rank") <= 5) \
                                                      .orderBy("Month", "rank")

    # Selecting the mentioned columns to display
    top_boroughs_monthly = top_boroughs_monthly.select("Pickup_Borough", "Month", col("count").alias("trip_count"))

    # Showing the result
    print("Top 5 popular pickup boroughs each month: ")
    top_boroughs_monthly.show(100)

    #TASK 3 - Top-K Processing - (2) Top 5 popular dropoff boroughs each month

    # Adding the month column
    final_df = final_df.withColumn("Month", month(col("date").cast("date")))

    # Group by 'Dropoff_Borough' and 'Month', count the number of trips
    dropoff_borough_monthly_trip_counts = final_df.groupBy("Dropoff_Borough", "Month").count()

    # Define window spec for ranking boroughs by trip count within each month
    windowSpec = Window.partitionBy("Month").orderBy(desc("count"))

    # Apply the window spec to rank the boroughs by trip count within each month
    top_dropoff_boroughs_monthly = dropoff_borough_monthly_trip_counts.withColumn("rank", rank().over(windowSpec)) \
                                                                       .filter(col("rank") <= 5) \
                                                                       .orderBy("Month", "rank")

    # Selecting the mentioned columns to display
    top_dropoff_boroughs_monthly = top_dropoff_boroughs_monthly.select("Dropoff_Borough", "Month", col("count").alias("trip_count"))

    # Show the result
    print("Top 5 popular dropoff boroughs each month: ")
    top_dropoff_boroughs_monthly.show(100)


    # TASK 3 - Top-K Processing - (3) Top 30 earnest routes

    # Adding a new column 'Route' that concatenates 'Pickup_Borough' and 'Dropoff_Borough'
    final_df_with_routes = final_df.withColumn("Route", concat_ws(" to ", "Pickup_Borough", "Dropoff_Borough"))
    
    # Group data by 'Route' and sum the 'driver_total_pay' to get the total profit for each route
    route_profit_grouped = final_df_with_routes.groupBy("Route").agg(_sum("driver_total_pay").alias("total_profit"))
    
    # Order the entire dataset by 'total_profit' in descending order
    top_routes = route_profit_grouped.orderBy(desc("total_profit")).limit(30)
    
    # Show the top 30 earnest routes without truncating the 'Route' column
    print("Top routes: ")
    top_routes.show(30, truncate=False)


    spark.stop()