import sys, string
import os
import socket
import time
import operator
import boto3
import json
from pyspark.sql.window import Window
from pyspark.sql.functions import rank
from pyspark.sql.functions import row_number
from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.types import FloatType
from pyspark.sql.functions import from_unixtime, date_format,concat
from pyspark.sql.functions import col, count, month , hour ,to_timestamp,lit
from pyspark.sql.functions import to_date, count, col,split,concat_ws,month,year
from pyspark.sql.functions import col, sum, avg,when
from graphframes import *



if __name__ == "__main__":

    spark = SparkSession\
        .builder\
        .appName("TestDataset")\
        .getOrCreate()
    
    
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
    
 # S3 data repository paths for the datasets
    rideshare_data_file_path = "ECS765/rideshare_2023/rideshare_data.csv"
    taxi_zone_lookup_file_path = "ECS765/rideshare_2023/taxi_zone_lookup.csv"

    rideshare_data_s3_path = f"s3a://{s3_data_repository_bucket}/{rideshare_data_file_path}"
    taxi_zone_lookup_s3_path = f"s3a://{s3_data_repository_bucket}/{taxi_zone_lookup_file_path}"

    
# As asked in Task 1.1
    # Loading rideshare_data.csv and taxi_zone_lookup.csv.
    rideshare_data_df = spark.read.csv(rideshare_data_s3_path, header=True, inferSchema=True)
    string_fields = ['pickup_location', 'dropoff_location', 'trip_length', 'request_to_pickup',
                     'total_ride_time', 'on_scene_to_pickup', 'on_scene_to_dropoff',
                     'passenger_fare', 'driver_total_pay', 'rideshare_profit']
    for field in string_fields:
        rideshare_data_df = rideshare_data_df.withColumn(field, col(field).cast('string'))

    taxi_zone_lookup_df = spark.read.csv(taxi_zone_lookup_s3_path, header=True, inferSchema=True)

    #As asked in task 1.2
        # Here we are Applying the join function based on fields pickup_location and dropoff_location of rideshare_data table 

     # Applying the join function based on fields pickup_location and dropoff_location of rideshare_data table
    full_rideshare_data_df = rideshare_data_df.join(
        taxi_zone_lookup_df.withColumnRenamed('Borough', 'Pickup_Borough')
                           .withColumnRenamed('Zone', 'Pickup_Zone')
                           .withColumnRenamed('service_zone', 'Pickup_service_zone'),
        rideshare_data_df["pickup_location"] == taxi_zone_lookup_df["LocationID"],
        'left'
    ).drop('LocationID')

   
    #As asked in Task 1.3  
        #Here we are converting the UNIX timestamp to the yyyy-MM-dd format.
        
    full_rideshare_data_df = full_rideshare_data_df.withColumn(
            'date',
            date_format(from_unixtime(col('date')), 'yyyy-MM-dd').cast('string')
        )
        
    #As asked in Task 1.4
    # Here we are printing the number of rows and schema of the new dataframe in the terminal
    print("Number of rows in the DataFrame:", full_rideshare_data_df.count())
    full_rideshare_data_df.printSchema()


    #########################################Task 2############################################################
   
    # Here we are Extracting the month from the date col and we have create a business-month combination
    full_rideshare_data_df = full_rideshare_data_df.withColumn('month', split(col('date'), '-')[1]) \
                                               .withColumn('business_month', concat_ws('-', col('business'), col('month')))
    

    trip_count_by_business_and_month = full_rideshare_data_df.groupBy('business_month') \
                                                           .agg(count('*').alias('trip_count'))
    
   
    

    s3_data_repository_bucket = os.environ['DATA_REPOSITORY_BUCKET']
    trip_count_by_business_and_month \
        .repartition(1) \
        .write \
        .option("header", "true") \
        .mode("overwrite") \
        .csv("s3a://" + s3_data_repository_bucket + "/ECS765/rideshare_2023/business_month_count_data.csv")
    
     # Show the aggregated data
    trip_count_by_business_and_month.orderBy(col('trip_count').desc()).show()


   # Converting 'rideshare_profit' column to float type so that any numerical operation can be performed
    full_rideshare_data_df = joined_data_final_sp.withColumn("rideshare_profit", col("rideshare_profit").cast(FloatType()))
        
   # Grouping by 'business_month',so that we can determine the sum of profit using 'rideshare_profit' column
    profits_by_business_and_month = joined_data_final_sp.groupBy('business_month') \
                                                        .agg(sum('rideshare_profit').alias('total_profit'))

   # Showing the calculated profits which are order by "buisness month"
    profits_by_business_and_month.orderBy('business_month').show(truncate=False)

    s3_data_repository_bucket = os.environ['DATA_REPOSITORY_BUCKET']  
    output_path = f"s3a://{s3_data_repository_bucket}/ECS765/rideshare_2023/profits_by_business_and_month.csv"
    profits_by_business_and_month \
        .repartition(1) \
        .write \
        .option("header", "true") \
        .mode("overwrite") \
        .csv(output_path)
    
    
    profits_by_business_and_month.show()

    
    full_rideshare_data_df = full_rideshare_data_df.withColumn("driver_total_pay", col("driver_total_pay").cast(FloatType()))
    
    
    full_rideshare_data_df = full_rideshare_data_df.withColumn(
        "business_month",
        concat_ws('-', col("business"), month(col("date")), year(col("date")))
    )
    
    
    total_earnings_of_driver = full_rideshare_data_df.groupBy('business_month') \
        .agg((sum('driver_total_pay') / 1000).alias('total_earnings_in_thousands'))
    
    
    total_earnings_of_driver = total_earnings_of_driver.orderBy('business_month')
    
        
    
    s3_data_repository_bucket =  os.environ['DATA_REPOSITORY_BUCKET']
    output_path = f"s3a://{s3_data_repository_bucket}/ECS765/rideshare_2023/total_earnings_of_driver.csv"
    
    
    total_earnings_of_driver \
        .repartition(1) \
        .write \
        .option("header", "true") \
        .mode("overwrite") \
        .csv(output_path)
    
    total_earnings_of_driver.show()



    ##############################Task 3#######################################################
    
    
    full_rideshare_data_df = full_rideshare_data_df.withColumn("Month", month(col("date")))
    
    
    borough_monthly_count = (full_rideshare_data_df
                             .groupBy("Pickup_Borough", "Month")
                             .agg(count("*").alias("trip_count")))
    
    
    windowSpec = Window.partitionBy("Month").orderBy(col("trip_count").desc())
    
    
    ranked_boroughs = (borough_monthly_count
                       .withColumn("rank", rank().over(windowSpec))
                       .filter(col("rank") <= 5)
                       .orderBy("Month", col("trip_count").desc()))
    
    
    final_result = ranked_boroughs.select("Pickup_Borough", "Month", "trip_count")
    
    final_result.show(30, truncate=False)

    
    full_rideshare_data_df = full_rideshare_data_df.withColumn('month', month(col('date')))
    
    
    monthly_borough_trip_counts = full_rideshare_data_df.groupBy('Dropoff_Borough', 'month') \
        .agg(count('*').alias('trip_count'))
    
    
    windowSpec = Window.partitionBy('month').orderBy(col('trip_count').desc())
    
    
    top_boroughs_by_month = monthly_borough_trip_counts.withColumn('rank', row_number().over(windowSpec)) \
        .filter(col('rank') <= 5) \
        .drop('rank') \
        .orderBy('month', col('trip_count').desc())
    

    top_boroughs_by_month.show(30, truncate=False)
    full_rideshare_data_df = full_rideshare_data_df.withColumn(
        'Route',
        concat_ws(' to ', col('Pickup_Borough'), col('Dropoff_Borough'))
    )
    
    
    route_profit_df = full_rideshare_data_df.groupBy('Route') \
        .agg(sum('driver_total_pay').alias('total_profit'))
    
    
    top_routes_df = route_profit_df.orderBy(col('total_profit').desc()).limit(30)
    
    top_routes_df.show(30, truncate=False)  

 ##################################################################Task 4 ###############################################################################33   
        
    
    full_rideshare_data_df = full_rideshare_data_df.withColumn('driver_total_pay', col('driver_total_pay').cast('float'))

    
    average_pay_by_time_of_day = full_rideshare_data_df.groupBy('time_of_day').agg(avg('driver_total_pay').alias('average_drive_total_pay'))
    
    
    average_pay_by_time_of_day_ordered = average_pay_by_time_of_day.orderBy(col('average_drive_total_pay').desc())
    
    
    average_pay_by_time_of_day_ordered.show()



    
    full_rideshare_data_df = full_rideshare_data_df.withColumn("trip_length", col("trip_length").cast("float"))
    
    
    average_trip_length_by_time_of_day = full_rideshare_data_df.groupBy('time_of_day').agg(avg('trip_length').alias('average_trip_length'))
    
    
    average_trip_length_by_time_of_day_ordered = average_trip_length_by_time_of_day.orderBy(col('average_trip_length').desc())
    
    
    average_trip_length_by_time_of_day_ordered.show()


    
    full_rideshare_data_df = full_rideshare_data_df.withColumn("driver_total_pay", col("driver_total_pay").cast("float"))
    full_rideshare_data_df = full_rideshare_data_df.withColumn("trip_length", col("trip_length").cast("float"))
    
    
    avg_driver_pay_by_time_of_day_df = full_rideshare_data_df.groupBy("time_of_day") \
        .agg(avg("driver_total_pay").alias("average_driver_total_pay"))
    
    avg_trip_length_by_time_of_day_df = full_rideshare_data_df.groupBy("time_of_day") \
        .agg(avg("trip_length").alias("average_trip_length"))
    
    
    avg_earnings_df = avg_driver_pay_by_time_of_day_df.join(avg_trip_length_by_time_of_day_df, "time_of_day")
    
    
    avg_earnings_per_mile_df = avg_earnings_df.withColumn(
        "average_earning_per_mile",
        col("average_driver_total_pay") / col("average_trip_length")
    )
    
    
    final_avg_earnings_per_mile_df = avg_earnings_per_mile_df.select("time_of_day", "average_earning_per_mile") \
        .orderBy(col("average_earning_per_mile").desc())
    
    # Showing the final DataFrame
    final_avg_earnings_per_mile_df.show()


#################################################5###########################################################

    # Ensuring the 'date' column is of type Date
    full_rideshare_data_df = full_rideshare_data_df.withColumn("date", col("date").cast(DateType()))

    # Filtering for January month
    january_data = full_rideshare_data_df.filter(month(col("date")) == 1)

    # Calculating the average waiting time ('request_to_pickup') for each day in January
    average_waiting_time_by_day = january_data.groupBy(dayofmonth(col("date")).alias("day")).agg(avg("request_to_pickup").alias("average_waiting_time"))

    # Sorting the result by day
    sorted_average_waiting_time_by_day = average_waiting_time_by_day.orderBy("day")

    s3_data_repository_bucket = os.environ['DATA_REPOSITORY_BUCKET']

    output_file_path = f"s3a://{s3_data_repository_bucket}/ECS765/average_waiting_time_january.csv"
    
    average_waiting_time_by_day \
        .repartition(1) \
        .write \
        .option("header", "true") \
        .mode("overwrite") \
        .csv(output_file_path)


    
#####################################################################6################################################################################
    
    trip_counts = full_rideshare_data_df.groupBy("Pickup_Borough", "time_of_day") \
        .agg(count("*").alias("trip_count"))
    
    
    filtered_trip_counts = trip_counts.filter((col("trip_count") > 0) & (col("trip_count") < 1000))
    
    
    filtered_trip_counts.show(truncate=False)

    
    
    evening_trips = full_rideshare_data_df.filter(col("time_of_day") == "evening")
    
    
    evening_trip_counts = evening_trips.groupBy("Pickup_Borough") \
        .agg(count("*").alias("trip_count"))
    
    
    evening_trip_counts = evening_trip_counts.withColumn("time_of_day", lit("evening"))
    

    evening_trip_counts = evening_trip_counts.select("Pickup_Borough", "time_of_day", "trip_count")
    
    
    evening_trip_counts.show(truncate=False)


    Filter trips that started in Brooklyn and ended in Staten Island
    brooklyn_to_staten_island_trips = full_rideshare_data_df.filter(
        (col("Pickup_Borough") == "Brooklyn") & (col("Dropoff_Borough") == "Staten Island")
    )
    
    
    brooklyn_to_staten_island_trips.select("Pickup_Borough", "Dropoff_Borough", "Pickup_Zone") \
        .show(10, truncate=False)
    
    
    trip_count = brooklyn_to_staten_island_trips.count()
    
    
    print(f"Total number of trips from Brooklyn to Staten Island: {trip_count}")
            

    
    spark.stop()