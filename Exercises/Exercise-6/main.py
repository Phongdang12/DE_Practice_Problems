from datetime import timedelta
from pyspark.sql import SparkSession, Window
import pyspark.sql.functions as F
import zipfile
import os
import tempfile
from pyspark.sql.types import DoubleType, IntegerType


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
# không thể truyền đường dẫn CSV bên trong ZIP cho Spark(df = spark.read.csv("data/file.zip/inside_file.csv")  # ❌ Lỗi!
# Mà phải đọc file csv rồi ghi ra một file tạm để nó đọc
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
                 .withColumn('birth_year',F.col('birth_year').cast(IntegerType()))\
                 .withColumnRenamed('bikeid','bike_id')\
                 .withColumn('tripduration',F.col('tripduration').cast(DoubleType()))\
                 .withColumnRenamed('tripduration','trip_duration')\
                 .withColumnRenamed('usertype','user_type')\
                 .withColumnRenamed('start_time','started_at')\
                 .withColumnRenamed('end_time','ended_at')\
                 .withColumnRenamed("from_station_name", "start_station_name")\
                 .withColumn('gender', F.when(F.col('gender').isNull(),'Unknown').otherwise(F.col('gender')))\
                 .withColumn('birth_year', F.when(F.col('birth_year').isNull(),'Unknown').otherwise(F.col('birth_year')))\
                 .filter(F.col('trip_duration')>0)                     
        if "ride_id" in df.columns:
            df=df.withColumn('end_station_id',F.col('end_station_id').cast(IntegerType()))\
                 .withColumn('rideable_type',F.initcap(F.col('rideable_type')))\
                 .withColumn('trip_duration', (F.unix_timestamp('ended_at') - F.unix_timestamp('started_at')))
        
        transformed_dataframes.append(df)

    return transformed_dataframes

 # 1. What is the `average` trip duration per day?
def avg_trip_duration_per_day(dataframes, output_path):
    combined_df = None
    for df in dataframes:
        df = df.select("started_at", "trip_duration")
        if combined_df is None:
            combined_df = df
        else:
            combined_df = combined_df.union(df)
    result=combined_df.groupBy(F.date_format("started_at","yyyy-MM-dd").alias("date"))\
                .agg(F.avg("trip_duration").alias("avg_duration"))\
                .orderBy("date")

    output_result(result, output_path, "trip_duration.csv")

# 2. How many trips were taken each day?
def trips_per_day(dataframes, output_path):
    combined_df = None
    for df in dataframes:
        df = df.select("started_at")

        if combined_df is None:
            combined_df = df
        else:
            combined_df = combined_df.union(df)

    result=combined_df.groupBy(F.date_format("started_at","yyyy-MM-dd").alias("date"))\
                .agg(F.count("*").alias("trip_count"))\
                .orderBy("date")

    output_result(result, output_path, "trips_per_day.csv")

# 3. What was the most popular starting trip station for each month?
def popular_month_station(dataframes, output_path):
    combined_df = None
    for df in dataframes:
        df = df.select("started_at", "start_station_name")

        if combined_df is None:
            combined_df = df
        else:
            combined_df = combined_df.union(df)

    per_month=combined_df.groupBy(F.date_format('started_at','yyyy-MM').alias('month'),F.col("start_station_name"))\
                .agg(F.count("*").alias("trip_count"))
    window_spec=Window.partitionBy("month").orderBy(F.desc("trip_count"))
    result=per_month.withColumn("rank_desc",F.row_number().over(window_spec))\
                    .filter(F.col("rank_desc")==1).select("month","start_station_name","trip_count")

    output_result(result, output_path, "popular_station_per_month.csv")

# 4. What were the top 3 trip stations each day for the last two weeks?
def top_station_by_day(dataframes, output_path):
    combined_df = None
    for df in dataframes:
        df = df.select("started_at", "start_station_name")
        if combined_df is None:
            combined_df = df
        else:
            combined_df = combined_df.union(df)

        recent_df = filter_last_two_weeks(combined_df, "started_at")

        daily_station_counts=recent_df.groupBy(F.date_format("started_at","yyyy-MM-dd").alias("date"),F.col("start_station_name"))\
                                      .agg(F.count("*").alias("trip_count"))
        
        window_spec=Window.partitionBy("date").orderBy(F.desc("trip_count"))
        result=daily_station_counts.withColumn("rank",F.row_number().over(window_spec))\
                                   .filter(F.col("rank")<=3)\
                                   .select("date","start_station_name","trip_count","rank")\
                                   .orderBy("date","rank")

        output_result(result, output_path, "top_station_by_day.csv")
    else:
        print("No data found for top_station_by_day analysis")



# 5. Do `Male`s or `Female`s take longer trips on average?
def longest_trips(dataframes, output_path):
    result = None
    for df in dataframes:
        if "trip_id" in df.columns:
            df_filtered = df.select("trip_duration","gender")
            result=df_filtered.filter(F.col("gender").isin("Male","Female"))\
                .groupBy("gender")\
                .agg(F.avg("trip_duration").alias("avg_duration"))\
                .orderBy(F.desc("avg_duration"))
            break

    if result:
        output_result(result, output_path, "longest_trips.csv")
    else:
        print("No data found for longest_trips analysis")

# 6. What is the top 10 ages of those that take the longest trips, and shortest?
def top_ages(dataframes, output_path):
    result = None
    for df in dataframes:
        if "trip_id" in df.columns:
            df_filtered = df.select("trip_duration","birth_year")

            df_with_age = df_filtered.filter(F.col("birth_year") != "Unknown")\
                       .withColumn("age", 2020 - F.col("birth_year"))
    
            avg_duration_by_age = df_with_age.groupBy("age")\
                                     .agg(F.avg("trip_duration").alias("avg_duration"))
    
            longest_trips = avg_duration_by_age.orderBy(F.desc("avg_duration"))\
                                       .limit(10)\
                                       .withColumn("trip_type", F.lit("longest"))
    
            shortest_trips = avg_duration_by_age.orderBy(F.asc("avg_duration"))\
                                        .limit(10)\
                                        .withColumn("trip_type", F.lit("shortest"))
    
            result = longest_trips.union(shortest_trips)
            break

    if result:
        output_result(result, output_path, "top_ages.csv")
    else:
        print("No data found for top_ages analysis")

# Spark thường chia dữ liệu thành nhiều phần để xử lý song song
# Nếu không có coalesce(1), Spark sẽ tạo ra nhiều file CSV (mỗi partition một file)
def output_result(result, output_path, name):
    try:
        result.coalesce(1).write.option("header", "true").csv(output_path)

        files = os.listdir(output_path)
#         output_path/
# ├── part-00000-xxx.csv  ← File CSV thực tế (tên random)
# ├── _SUCCESS            ← File báo hiệu ghi thành công
# └── .crc files          ← File checksum (có thể có)
# ->dọn dẹp các file không cần thiết
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
        max_date = df.agg({start: "max"}).collect()[0][0]
        end_date = max_date
        start_date = max_date - timedelta(days=14)
        filtered_data = df.filter((F.col(start) >= start_date) & (F.col(start) <= end_date))
        return filtered_data
    except Exception as e:
        print(e)
        return df


if __name__ == "__main__":
    main()