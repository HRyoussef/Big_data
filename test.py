from pyspark import SparkContext
import time

sc = SparkContext("local", "test")
print(sc.appName)
time.sleep(2000)