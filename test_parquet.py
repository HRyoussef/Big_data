import pandas as pd
import time
import psutil
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg

file = "yellow_tripdata_2026-01.parquet"

process = psutil.Process(os.getpid())

# ======================
# 🐼 PANDAS
# ======================
mem_before = process.memory_info().rss / 1024 / 1024
start = time.time()

df_pandas = pd.read_parquet(file)
avg_fare_pandas = df_pandas["fare_amount"].mean()

end = time.time()
mem_after = process.memory_info().rss / 1024 / 1024

print("PANDAS")
print("Avg fare:", avg_fare_pandas)
print("Temps:", round(end - start, 2), "s")
print("RAM:", round(mem_after - mem_before, 2), "MB")

print("##########################################################")

# ======================
# ⚡ PYSPARK
# ======================

# démarrage Spark (hors mesure)
spark = SparkSession.builder \
    .appName("compare") \
    .master("local[*]") \
    .getOrCreate()

start = time.time()

df_spark = spark.read.parquet(file)
avg_fare_spark = df_spark.select(avg("fare_amount")).collect()[0][0]

end = time.time()

print("PYSPARK")
print("Avg fare:", avg_fare_spark)
print("Temps:", round(end - start, 2), "s")
print("RAM: non fiable (Spark utilise JVM)")

spark.stop()