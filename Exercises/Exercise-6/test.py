from datetime import datetime, timedelta
from pyspark.shell import spark
from pyspark.sql import functions as F
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import to_date, col, unix_timestamp, month, rank, count, row_number, avg,when, to_timestamp, initcap, date_format,desc, asc, lit,isnull
import zipfile
import os
import tempfile
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType


def main():
    combination_of_question("./data", "reports/analysis_1", "reports/analysis_2", "reports/analysis_3",
                            "reports/analysis_4", "reports/analysis_5", "reports/analysis_6")


def combination_of_question(directory_path, path_01, path_02, path_03, path_04, path_05, path_06):
    df = create_data_frame(directory_path)
    avg_trip_duration_per_day(df, path_01)
    trips_per_day(df, path_02)
    popular_month_station(df, path_03)
    top_station_by_day(df, path_04)
    longest_trips(df, path_05)
    top_ages(df, path_06)


def create_data_frame(directory_path):
    spark = SparkSession.builder.appName("Exercise6").getOrCreate()
    dataframes = []

    for filename in os.listdir(directory_path):
        if filename.endswith(".zip"):
            zip_file_path = os.path.join(directory_path, filename)
            with zipfile.ZipFile(zip_file_path, 'r') as zip_file:
                for file_info in zip_file.infolist():
                    if '__MACOSX' not in file_info.filename and file_info.filename.endswith(".csv"):
                        with tempfile.NamedTemporaryFile(delete=False) as temp_csv_file:
                            with zip_file.open(file_info) as csv_file:
                                temp_csv_file.write(csv_file.read())

                        df = spark.read.csv(temp_csv_file.name, header=True, inferSchema=True)
                        dataframes.append(df)
    
    # Transform dataframes and store them back
    transformed_dataframes = []
    for df in dataframes:
        if "trip_id" in df.columns:
            df=df.withColumnRenamed('birthyear','birth_year')\
                 .withColumn('birth_year',col('birth_year').cast(IntegerType()))\
                 .withColumnRenamed('bikeid','bike_id')\
                 .withColumn('tripduration',col('tripduration').cast(DoubleType()))\
                 .withColumnRenamed('tripduration','trip_duration')\
                 .withColumnRenamed('usertype','user_type')\
                 .withColumnRenamed('start_time','started_at')\
                 .withColumnRenamed('end_time','ended_at')\
                 .withColumnRenamed("from_station_name", "start_station_name")\
                 .withColumn('gender', when(col('gender').isNull(),'Unknown').otherwise(col('gender')))\
                 .withColumn('birth_year', when(col('birth_year').isNull(),'Unknown').otherwise(col('birth_year')))\
                 .filter(col('trip_duration')>0)                     
        if "ride_id" in df.columns:
            df=df.withColumn('end_station_id',col('end_station_id').cast(IntegerType()))\
                 .withColumn('rideable_type',initcap(col('rideable_type')))\
                 .withColumn('trip_duration', (unix_timestamp('ended_at') - unix_timestamp('started_at')))
        
        transformed_dataframes.append(df)

    return transformed_dataframes


def avg_trip_duration_per_day(dataframes, output_path):
    combined_df = None
    for df in dataframes:
        df = df.select("started_at", "trip_duration")
        if combined_df is None:
            combined_df = df
        else:
            combined_df = combined_df.union(df)
    result=combined_df.groupBy(date_format("started_at","yyyy-MM-dd").alias("date"))\
                .agg(avg("trip_duration").alias("avg_duration"))\
                .orderBy("date")

    output_result(result, output_path, "trip_duration.csv")


def trips_per_day(dataframes, output_path):
    combined_df = None
    for df in dataframes:
        df = df.select("started_at")

        if combined_df is None:
            combined_df = df
        else:
            combined_df = combined_df.union(df)

    result=combined_df.groupBy(date_format("started_at","yyyy-MM-dd").alias("date"))\
                .agg(count("*").alias("trip_count"))\
                .orderBy("date")

    output_result(result, output_path, "trips_per_day.csv")


def popular_month_station(dataframes, output_path):
    combined_df = None
    for df in dataframes:
        df = df.select("started_at", "start_station_name")

        if combined_df is None:
            combined_df = df
        else:
            combined_df = combined_df.union(df)

    per_month=combined_df.groupBy(date_format('started_at','yyyy-MM').alias('month'),col("start_station_name"))\
                .agg(count("*").alias("trip_count"))
    window_spec=Window.partitionBy("month").orderBy(desc("trip_count"))
    result=per_month.withColumn("rank_desc",row_number().over(window_spec))\
                    .filter(col("rank_desc")==1).select("month","start_station_name","trip_count")

    output_result(result, output_path, "popular_station_per_month.csv")


def top_station_by_day(dataframes, output_path):
    combined_df = None
    for df in dataframes:
        df = df.select("started_at", "start_station_name")
        if combined_df is None:
            combined_df = df
        else:
            combined_df = combined_df.union(df)

        recent_df = filter_last_two_weeks(combined_df, "started_at")

        daily_station_counts=recent_df.groupBy(date_format("started_at","yyyy-MM-dd").alias("date"),col("start_station_name"))\
                                      .agg(count("*").alias("trip_count"))
        
        window_spec=Window.partitionBy("date").orderBy(desc("trip_count"))
        result=daily_station_counts.withColumn("rank",row_number().over(window_spec))\
                                   .filter(col("rank")<=3)\
                                   .select("date","start_station_name","trip_count","rank")\
                                   .orderBy("date","rank")

        output_result(result, output_path, "top_station_by_day.csv")
    else:
        print("No data found for top_station_by_day analysis")




def longest_trips(dataframes, output_path):
    result = None
    for df in dataframes:
        if "trip_id" in df.columns:
            df_filtered = df.select("trip_duration","gender")
            result=df_filtered.filter(col("gender").isin("Male","Female"))\
                .groupBy("gender")\
                .agg(avg("trip_duration").alias("avg_duration"))\
                .orderBy(desc("avg_duration"))
            break

    if result:
        output_result(result, output_path, "longest_trips.csv")
    else:
        print("No data found for longest_trips analysis")


def top_ages(dataframes, output_path):
    result = None
    for df in dataframes:
        if "trip_id" in df.columns:
            df_filtered = df.select("trip_duration","birth_year")

            df_with_age = df_filtered.filter(col("birth_year") != "Unknown")\
                       .withColumn("age", 2020 - col("birth_year"))
    
            avg_duration_by_age = df_with_age.groupBy("age")\
                                     .agg(avg("trip_duration").alias("avg_duration"))
    
            longest_trips = avg_duration_by_age.orderBy(desc("avg_duration"))\
                                       .limit(10)\
                                       .withColumn("trip_type", lit("longest"))
    
            shortest_trips = avg_duration_by_age.orderBy(asc("avg_duration"))\
                                        .limit(10)\
                                        .withColumn("trip_type", lit("shortest"))
    
            result = longest_trips.union(shortest_trips)
            break

    if result:
        output_result(result, output_path, "top_ages.csv")
    else:
        print("No data found for top_ages analysis")


def output_result(result, output_path, name):
    try:
        result.coalesce(1).write.option("header", "true").csv(output_path)

        files = os.listdir(output_path)
        for file in files:
            if file.endswith(".csv"):
                csv_file_path = os.path.join(output_path, file)
                new_csv_file_path = os.path.join(output_path, name)
                os.rename(csv_file_path, new_csv_file_path)
            else:
                file_to_delete = os.path.join(output_path, file)
                os.remove(file_to_delete)

    except Exception as e:
        if "exists" in str(e):
            print(f"!!!File {name} already exists!!!")
        else:
            print(e)

def filter_last_two_weeks(df, start):
    try:
        max_date = df.agg({df:"max"}).collect()[0][0]
        end_date = max_date
        start_date = max_date - timedelta(days=14)
        filtered_data = df.filter((col(start) >= start_date) & (col(start) <= end_date))
        return filtered_data
    except Exception as e:
        print(e)
        return df


if __name__ == "__main__":
    main()