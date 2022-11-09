import pyspark
import pandas
from pyspark.sql import SparkSession
import findspark
from datetime import datetime

findspark.init()

spark = SparkSession.builder.master("local[*]").getOrCreate()
spark.conf.set("spark.sql.repl.eagerEval.enabled", True) # type: ignore # Property used to format output tables better

data_df = spark.read.csv('data/underdrain.csv', header=True, inferSchema=True)  # type: ignore
data_df.printSchema()
data_df.toPandas()
data_df.filter(data_df.SHAPE_Length<100).show(10)


# data_df.drop('DCH_GRPH_KEY', 'DCH_DSTNTN_TYPE', 'DCH_UPS_ELEV_FT_NBR', 'DCH_DNS_ELEV_FT_NBR', 'DCH_STREAM_NAME', 'DCH_FEA_KEY')

select = data_df.select(
    'OBJECTID','DCH_INSTALL_DATE','SHAPE_Length','DCH_OWNER_NAME'
)
select.agg({'SHAPE_Length':'avg'})