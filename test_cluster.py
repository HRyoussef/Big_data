from pyspark.sql import SparkSession
import time

spark = SparkSession.builder \
    .appName("TaxiCluster") \
    .master("spark://172.22.114.75:7077") \
    .getOrCreate()

print("Master :", spark.sparkContext.master)

start = time.time()
df = spark.read.parquet("/home/youssef.hirchaou@Digital-Grenoble.local/campus/Big_data_architecture_distribuées/yellow_tripdata_2026-01.parquet")
print("Nombre de lignes :", df.count())
end = time.time()

print(f"Temps cluster : {end - start:.2f} secondes")
spark.stop()
