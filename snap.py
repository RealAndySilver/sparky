from datetime import datetime
import math
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
import findspark
findspark.init()

spark = SparkSession.builder.master("local[*]").getOrCreate()
spark.conf.set("spark.sql.repl.eagerEval.enabled", "true")  

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

def snap_row(date, resolution=5):
    """Snap row to resolution"""
    timestamp = get_timestamp(date)
    snapped_time = snap_time_to_resolution(timestamp, resolution)
    return snapped_time

snap_udf = udf(snap_row, 'timestamp')

data_df = spark.read.csv('data/underdrain.csv',
                         header=True,
                         inferSchema=True)  # type: ignore

# data_df.na.drop(subset=['DCH_INSTALL_DATE']).show(truncate=False)
data_df.printSchema()
filtered_df = data_df.filter(data_df.DCH_INSTALL_DATE.isNotNull())

# filtered_df.foreach(lambda row: snap_row(row[18], 1))  # type: ignore

# filtered_df.withColumn('SNAPPED_TIME', snap_row(filtered_df.DCH_INSTALL_DATE, 1)).show(5)    # type: ignore
filtered_df.withColumn('SNAPPED_TIME', snap_udf(filtered_df.DCH_INSTALL_DATE)).write.csv("data/csvs", header=True)
# filtered_df.show(10)
# filtered_df.write.csv("data/csvs", header=True)
