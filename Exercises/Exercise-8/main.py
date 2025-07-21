import duckdb
import os

def create_ddl():
    con=duckdb.connect(':memory:') #Lưu trong bộ nhớ->kết thúc khi tắt chương trình
    # con = duckdb.connect('C:/path/to/database.db') #Lưu trên ổ đĩa
    con.execute("""
        CREATE OR REPLACE TABLE electric_vehicle(
            vin_id VARCHAR(10),
            country VARCHAR(50),
            city VARCHAR(50),
            state CHAR(2),
            postal_code INTEGER,
            model_year INTEGER,
            make VARCHAR(50),
            model VARCHAR(50),
            electric_type VARCHAR(50),
            cafv_eligibility VARCHAR(100),
            electric_range INTEGER,
            base_msrp INTEGER,
            legislative_district DECIMAL(5,2),
            dol_id BIGINT,
            vehicle_location VARCHAR(100),
            electric_utility VARCHAR(100),
            census_tract BIGINT );
                """)
    
    return con

def insert_con(con):
    insert_data = """
        INSERT INTO electric_vehicle
        SELECT 
            "VIN (1-10)" AS vin_id,
            Country AS country,
            City AS city,
            State AS state,
            "Postal Code" AS postal_code,
            "Model Year" AS model_year,
            Make AS make,
            CASE
            WHEN "Model" IS NULL THEN 'Unknown'
            WHEN "Model" LIKE 'MODEL%' THEN substring("Model",6)
            ELSE "Model"
            END AS model,
            "Electric Vehicle Type" AS electric_type,
            "Clean Alternative Fuel Vehicle (CAFV) Eligibility" AS cafv_eligibility,
            "Electric Range" AS electric_range,
            "Base MSRP" AS base_msrp,
            CAST(CASE
            WHEN "Legislative District" IS NULL THEN 0
            ELSE "Legislative District"
            END AS DECIMAL(5,2) ) AS legislative_district,
            CAST("DOL Vehicle ID" AS BIGINT) AS dol_id,
            "Vehicle Location"  AS vehicle_location,
            CASE WHEN "Electric Utility" IS NULL THEN 'Unknown'  
            ELSE "Electric Utility"
            END AS electric_utility,
            CAST("2020 Census Tract" AS BIGINT) AS census_tract

        FROM read_csv_auto('data/Electric_Vehicle_Population_Data.csv');
    """
    con.execute(insert_data)
    return con

def query_data(con):
    # 1. Count the number of electric cars per city
    print("=== Count of electric cars per city ===")
    num_cars_per_city = con.execute("""
           SELECT city, COUNT(*) AS num_cars
           FROM electric_vehicle
           GROUP BY city
           ORDER BY num_cars DESC
           LIMIT 10;          
    """).df()
    print(num_cars_per_city)

    # 2. Find the top 3 most popular electric vehicles.
    print("\n=== Top 3 most popular electric vehicles ===")
    top_3_vehicles=con.execute("""
            SELECT model, COUNT(*) AS num_vehicles
            FROM electric_vehicle
            GROUP BY model
            ORDER BY num_vehicles DESC
            LIMIT 3;
    """).df()
    print(top_3_vehicles)

    # 3. Find the most popular electric vehicle in each postal code.
    print("\n=== Most popular electric vehicle in each postal code ===")
    most_popular_per_postal_code=con.execute("""
            WITH RankedVehicles AS (
            SELECT postal_code,model, COUNT(*) AS num_vehicles,
            DENSE_RANK() OVER (PARTITION BY postal_code ORDER BY COUNT(*) DESC) AS rank
            FROM electric_vehicle
            GROUP BY postal_code,model  ) 
                                             
            SELECT postal_code, model, num_vehicles
            FROM RankedVehicles
            WHERE rank = 1
            ORDER BY num_vehicles DESC
            LIMIT 10;                                 
                                      """).df()
    print(most_popular_per_postal_code)

    # 4.Count the number of electric cars by model year and save as parquet files partitioned by year.
    print("\n=== Count of electric cars by model year ===")
    count_by_year=con.execute("""
        SELECT model,model_year,COUNT(*) as num_cars
        FROM electric_vehicle
        GROUP BY model, model_year
        ORDER BY model_year DESC;
""").df()
    print(count_by_year)

    con.execute("""
        COPY(
            SELECT model,model_year,COUNT(*) as num_cars
            FROM electric_vehicle
            GROUP BY model, model_year
            ORDER BY model_year DESC
        ) TO 'output' (FORMAT PARQUET, PARTITION_BY model_year);
    """)
    print("\nData saved to 'output' partitioned by model year.\n")
def main():
    os.makedirs('output', exist_ok=True)
    
    con=create_ddl()
    con=insert_con(con)
    result=query_data(con)
    con.close()

if __name__ == "__main__":
    main()
