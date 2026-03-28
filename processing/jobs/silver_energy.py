import os
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
from .spark_session import spark
from datetime import date, timedelta

account = os.getenv('AZURE_STORAGE_ACCOUNT_NAME')
target_date = date.today() - timedelta(days=5)

bronze_path = (
    f"abfss://bronze@{account}.dfs.core.windows.net/energy/"
    f"year={target_date.year}/"
    f"month={target_date.month:02d}/"
    f"day={target_date.day:02d}/"
)

silver_path = (
    f"abfss://silver@{account}.dfs.core.windows.net/energy/"
    f"year={target_date.year}/"
    f"month={target_date.month:02d}/"
    f"day={target_date.day:02d}/"
)

df = spark.read.json(bronze_path)

df_silver = (
    df
    .dropDuplicates(["city_name", "country", "time"])
    .dropna(subset=["city_name", "country", "time", "load_mw"])
    .withColumn("time", F.to_timestamp("time"))
    .withColumn("load_mw", F.col("load_mw").cast(DoubleType()))
    .withColumn("year", F.year("time"))
    .withColumn("month", F.month("time"))
    .withColumn("day", F.dayofmonth("time"))
    .select("city_name", "country", "time", "load_mw", "year", "month", "day")
)

count = df_silver.count()

df_silver.write.mode("append").partitionBy("year", "month", "day").parquet(silver_path)

print(f"Written {count} rows to silver layer.")
