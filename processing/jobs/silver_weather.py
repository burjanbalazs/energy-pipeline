import os
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType
from .spark_session import spark

account = os.getenv('AZURE_STORAGE_ACCOUNT_NAME')
bronze_path = f"abfss://bronze@{account}.dfs.core.windows.net/weather"
silver_path = f"abfss://silver@{account}.dfs.core.windows.net/weather"

df = spark.read.json(bronze_path)
df_silver = (
    df
    .dropDuplicates(["city_name", "country", "time"])
    .dropna(subset=["city_name", "country", "time", "temperature"])
    .withColumn("time", F.to_timestamp("time"))
    .withColumn("latitude", F.col("latitude").cast(DoubleType()))
    .withColumn("longitude", F.col("longitude").cast(DoubleType()))
    .withColumn("temperature", F.col("temperature").cast(DoubleType()))
    .withColumn("humidity", F.col("humidity").cast(IntegerType()))
    .withColumn("wind_speed", F.col("wind_speed").cast(DoubleType()))
    .withColumn("precipitation", F.col("precipitation").cast(DoubleType()))
    .withColumn("weather_code", F.col("weather_code").cast(IntegerType()))
    .withColumn("year", F.year("time"))
    .withColumn("month", F.month("time"))
    .withColumn("day", F.dayofmonth("time"))
    .select("city_name", "country", "latitude", "longitude", "time",
            "temperature", "humidity", "wind_speed", "precipitation",
            "weather_code", "weather", "year", "month", "day")
)
count = df_silver.count()

df_silver.write.mode("overwrite").partitionBy("year", "month", "day").parquet(silver_path)

print(f"Written {count} rows to silver layer.")