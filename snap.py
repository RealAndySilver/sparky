from datetime import datetime
import math
from pyspark.sql import SparkSession
import findspark
findspark.init()

spark = SparkSession.builder.master("local[*]").getOrCreate()
spark.conf.set("spark.sql.repl.eagerEval.enabled", True)  # type: ignore


def get_time(date_text):
    """Get time from date text"""
    try:
        date_text = datetime.strptime(date_text, '%Y/%m/%d %H:%M:%S+00')      
    except ValueError:
        date_text = datetime.strptime("1900/01/01 01:01:01+00", '%Y/%m/%d %H:%M:%S+00')
    
    return date_text

def snap_time(timestamp, resolution=1):
    """Snap time to resolution"""
    if resolution <= 0:
        resolution = 1
    resolution_ms = resolution * 60 * 1000
    res = datetime.fromtimestamp(math.floor(timestamp / resolution_ms) * resolution_ms)
    print(res)
    return res

data_df = spark.read.csv('data/underdrain.csv',
                         header=True,
                         inferSchema=True)  # type: ignore
data_df.printSchema()
# data_df.na.drop(subset=['DCH_INSTALL_DATE']).show(truncate=False)
filtered_df = data_df.filter(data_df.DCH_INSTALL_DATE != 'None')

# data_df.toPandas()

filtered_df.foreach(
    lambda x:
        snap_time(
            get_time(x[18]).timestamp(),
            5  # type: ignore
        )
    )
