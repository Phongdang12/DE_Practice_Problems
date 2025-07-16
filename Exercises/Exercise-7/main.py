from pyspark.sql import SparkSession,Window
import pyspark.sql.functions as F
from pyspark.sql.types import DateType, LongType
import os
import zipfile 
import tempfile

   
def main():
    # your code here
    directory_path = "./data"
    df = create_data_frame(directory_path)
    df = add_col_source_file(df, "hard-drive-2022-01-01-failures.csv")
    df = add_col_file_date(df)
    df = add_col_brand(df)
    df = add_col_capacity_bytes(df)
    df = add_col_primary_key(df)
    os.makedirs("result", exist_ok=True)
    output_path = "result"
    output_result(df, output_path, "hard-drive-2022-01-01-failures.csv")



def create_data_frame(directory_path):
    spark = SparkSession.builder.appName("Exercise7").enableHiveSupport().getOrCreate()
    for file_name in os.listdir(directory_path):
        if file_name.endswith(".zip"):
            zip_file_path=os.path.join(directory_path,file_name)
            with zipfile.ZipFile(zip_file_path,'r') as zip_file:
                for file_info in zip_file.infolist():
                    if '__MACOSX' not in file_info.filename and file_info.filename.endswith('.csv'):
                        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
                            with zip_file.open(file_info) as csv_file:
                                temp_file.write(csv_file.read())
                        
                        df= spark.read.csv(temp_file.name, header=True,inferSchema=True)
    return df


def add_col_source_file(df,file_name):
    df=df.withColumn("source_file",F.lit(file_name))
    return df


def add_col_file_date(df):
    df = df.withColumn("file_date", F.substring(F.col("source_file"), 11, 10))
    df = df.withColumn("file_date",F.to_date(F.col("file_date"),"yyyy-MM-dd"))
    return df

def add_col_brand(df):
    df = df.withColumn("brand", 
        F.when(F.size(F.split(F.col("model"), " ")) > 1, 
               F.split(F.col("model"), " ")[0])
        .otherwise("unknown")
    )
    return df

def add_col_capacity_bytes(df):
    df_second = df.select(F.col("model"), F.col("capacity_bytes").cast(LongType()).alias("capacity_bytes"))\
                  .groupBy("model").agg(F.avg("capacity_bytes").alias("avg_capacity_bytes"))
    window_spec = Window.orderBy(F.col("avg_capacity_bytes").desc())
    df_second = df_second.withColumn("storage_ranking", F.row_number().over(window_spec))
    df = df.join(df_second.select("model", "storage_ranking"), on="model", how="left")
    return df
    

def add_col_primary_key(df):
    df = df.withColumn("primary_key", 
        F.md5(F.concat_ws("-", F.col("date"), F.col("serial_number"), F.col("model")))
    )
    return df

def output_result(df,output_path,name):
    df.coalesce(1).write.mode("overwrite").option("header","True").csv(output_path)
    file=os.listdir(output_path)
    for f in file:
        if f.endswith(".csv"):
            csv_file_path=os.path.join(output_path,f)
            new_csv_file_path=os.path.join(output_path,name)
            os.rename(csv_file_path,new_csv_file_path)
        else:
            file_to_delete=os.path.join(output_path,f)
            os.remove(file_to_delete)

    
if __name__ == "__main__":
    main()
