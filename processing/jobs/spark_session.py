import os
from pyspark.sql import SparkSession
from dotenv import load_dotenv
load_dotenv()

# Create Spark session
account_name = os.getenv('AZURE_STORAGE_ACCOUNT_NAME')
account_key = os.getenv('AZURE_STORAGE_ACCOUNT_KEY')

spark = SparkSession.builder \
    .appName("silver_weather") \
    .config(f"spark.hadoop.fs.azure.account.key.{account_name}.dfs.core.windows.net", account_key) \
    .config(f"spark.hadoop.fs.azure.account.auth.type.{account_name}.dfs.core.windows.net", "SharedKey") \
    .getOrCreate()

# Configure Hadoop for Azure Blob Storage
#spark._jsc.hadoopConfiguration().set(f"fs.azure.account.key.{os.getenv('AZURE_STORAGE_ACCOUNT_NAME')}.blob.core.windows.net", os.getenv('AZURE_STORAGE_ACCOUNT_KEY'))