# etl_jobs.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp
from pyspark.sql.dataframe import DataFrame

from spark_delta_config import read_from_postgres_with_partition
from spark_delta_cleaning import clean_clients_data, clean_transactions_data
from spark_delta import write_to_delta, deltalake_bronze_path