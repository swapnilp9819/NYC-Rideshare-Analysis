import sys, string
import os
import socket
import time
import operator
import boto3
import json
from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.functions import from_unixtime, date_format, to_date, count, col, month, year, sum as _sum
#from pyspark.sql.functions import to_date, count, col
#from pyspark.sql.functions import from_unixtime, to_date, month, col, sum as _sum, year
from graphframes import *
from pyspark.sql.functions import col, from_unixtime
from pyspark.sql.functions import concat_ws


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

    # Task 2 - Aggregation of Data
    
    # Extract year and month from the 'date' field
    final_df = final_df.withColumn("year", year(col("date")))
    final_df = final_df.withColumn("month", month(col("date")))
    
    # Create a 'business_month' column for aggregation
    final_df = final_df.withColumn("business_month", concat_ws("-", col("business"), col("month")))
    
    # Convert profit and pay columns to float for aggregation
    final_df = final_df.withColumn("rideshare_profit", col("rideshare_profit").cast("float"))
    final_df = final_df.withColumn("driver_total_pay", col("driver_total_pay").cast("float"))
    
    # 1. Number of trips per business per month
    trips_per_business_month = final_df.groupBy("business_month").count().withColumnRenamed("count", "trip_count")
    
    # 2. Total platform profits per business per month
    profits_per_business_month = final_df.groupBy("business_month").agg(_sum("rideshare_profit").alias("total_profit"))
    
    # 3. Total driver earnings per business per month
    earnings_per_business_month = final_df.groupBy("business_month").agg(_sum("driver_total_pay").alias("total_earnings"))
    
    # Showing dataframes for verification before exporting
    trips_per_business_month.show()
    profits_per_business_month.show()
    earnings_per_business_month.show()

    # Export answers to s3_bucket
    trips_per_business_month.coalesce(1).write.mode("overwrite").options(header=True).csv(f"s3a://{s3_bucket}/Aggregation_of_Data_csv_output_02/trip_count")
    profits_per_business_month.coalesce(1).write.mode("overwrite").options(header=True).csv(f"s3a://{s3_bucket}/Aggregation_of_Data_csv_output_02/total_profit")
    earnings_per_business_month.coalesce(1).write.mode("overwrite").options(header=True).csv(f"s3a://{s3_bucket}/Aggregation_of_Data_csv_output_02/total_earnings")

    spark.stop()