from pyspark.sql import SparkSession
import math
from datetime import datetime
from pyspark.sql.types import TimestampType
from pyspark.sql.functions import udf

spark = SparkSession.builder.master("local").getOrCreate()
sc = spark.sparkContext

data_df = spark.read.csv('data/underdrain.csv',
                         header=True,
                         inferSchema=True)
data_df.printSchema()
filtered_df = data_df.filter(data_df.DCH_INSTALL_DATE.isNotNull())

def get_timestamp(date_text):
    """Get timestamp from date text"""
    try:
        date_text = datetime.strptime(date_text, '%Y/%m/%d %H:%M:%S+00')
    except ValueError:
        date_text = datetime.strptime(
            "1900/01/01 01:01:01+00", '%Y/%m/%d %H:%M:%S+00')

    return date_text.timestamp()

def snap_time_to_resolution(timestamp, resolution=1):
    """Snap time to resolution"""
    if resolution <= 0:
        resolution = 1
    resolution_ms = resolution * 60
    snapped_time = datetime.fromtimestamp(
        math.floor(timestamp / resolution_ms) * resolution_ms)

    return snapped_time

@udf(returnType=TimestampType())
def snap_row(date, resolution=15):
    """Snap row to resolution"""
    timestamp = get_timestamp(date)
    snapped_time = snap_time_to_resolution(timestamp, resolution)
    return snapped_time

filtered_df.withColumn('SNAPPED_TIME', snap_row(filtered_df.DCH_INSTALL_DATE)).write.mode("overwrite").csv("data/csvs", header=True)

# @udf(returnType=TimestampType())
# def snap_row(date, resolution=5):
#     """Snap row to resolution"""
#     timestamp = get_timestamp(date)
#     snapped_time = snap_time_to_resolution(timestamp, resolution)
#     return snapped_time
# 
# filtered_df.withColumn('SNAPPED_TIME', snap_row(data_df.DCH_INSTALL_DATE)).write.mode("overwrite").csv("data/csvs", header=True)