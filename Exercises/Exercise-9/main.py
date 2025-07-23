import polars as pl

def load_data():
    # Load the CSV file into a lazy DataFrame with proper data types
    df_lazy = pl.scan_csv(
        "data/202306-divvy-tripdata.csv",
        schema_overrides={
            "started_at": pl.Datetime,
            "ended_at": pl.Datetime,
            "start_station_id": pl.String,
            "end_station_id": pl.String
        }
    )
    return df_lazy

def analyze_data(df_lazy):
    # eager=True: Thực thi ngay lập tức và trả về kết quả
    # eager=False (mặc định): Tạo lazy query, chỉ thực thi khi cần

    # 1,Count the number bike rides per day.
    sum_per_day=(
        df_lazy
        .with_columns(pl.col("started_at").dt.date().alias("ride_date"))
        .group_by("ride_date")
        .agg(pl.len().alias("total_rides"))
        .sort("ride_date")
        .collect()
    )
    print("==== Count the number bike rides per day. ====")
    print(sum_per_day)
    

    # 2,Calculate the average, max, and minimum number of rides per week of the dataset.
    
    week_summary=(
        df_lazy
        .with_columns(pl.col("started_at").dt.week().alias("ride_week"))
        .group_by("ride_week")
        .agg([
            pl.len().alias("total_rides")
        ])
        .select([
            pl.mean("total_rides").alias("avg_rides"),
            pl.max("total_rides").alias("max_rides"),
            pl.min("total_rides").alias("min_rides")
        ])
        .collect()
    )
    print("==== Calculate the average, max, and minimum number of rides per week of the dataset. ====")
    print(week_summary)

    # 3,For each day, calculate how many rides that day is above or below the same day last week.
    # Lệnh shift() trong Polars được sử dụng để dịch chuyển các giá trị trong một cột theo một khoảng nhất định.Như kiểu,Thêm 1 cột mới <-> cột cũ dịch chuyển n hàng lên hoặc xuống 

    daily_comparison=(
        df_lazy
        .with_columns(pl.col("started_at").dt.date().alias("ride_date"))
        .group_by("ride_date")
        .agg(pl.len().alias("total_rides"))
        .sort("ride_date")
        .with_columns(
            pl.col("total_rides").shift(7).alias("total_rides_last_week"),
        )
        .select([pl.col("total_rides"),
                 pl.col("total_rides_last_week"),
                 pl.col("ride_date"),
                 (pl.col("total_rides").cast(pl.Int64) - pl.col("total_rides_last_week").cast(pl.Int64)).alias("diff_to_last_week")])
        # .head(10)
        .collect()
    )
    print("==== Daily comparison to same day last week ====")
    print(daily_comparison)

def main():
    df_lazy=load_data()
    analyze_data(df_lazy)
    
if __name__ == "__main__":
    main()
