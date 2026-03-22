
import os
from pyspark.sql import SparkSession
from dotenv import load_dotenv
load_dotenv()

# Create Spark session
spark = SparkSession.builder \
    .appName("silver_weather") \
    .config(f"fs.azure.account.key.{os.getenv('AZURE_STORAGE_ACCOUNT_NAME')}.blob.core.windows.net", os.getenv('AZURE_STORAGE_ACCOUNT_KEY')) \
    .getOrCreate()

# Configure Hadoop for Azure Blob Storage
#spark._jsc.hadoopConfiguration().set(f"fs.azure.account.key.{os.getenv('AZURE_STORAGE_ACCOUNT_NAME')}.blob.core.windows.net", os.getenv('AZURE_STORAGE_ACCOUNT_KEY'))