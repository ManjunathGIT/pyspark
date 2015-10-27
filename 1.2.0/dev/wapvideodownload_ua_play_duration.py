from pyspark import SparkConf, SparkContext
from pyspark.sql import HiveContext
from decimal import Decimal
from datetime import datetime, date
from pyspark.sql import StructType, StructField, ByteType, ShortType, IntegerType, LongType, FloatType, DoubleType, DecimalType, StringType, BooleanType, TimestampType, DateType, ArrayType, MapType

conf = SparkConf().setAppName("spark_sql_datatype")

sc = SparkContext(conf=conf)

hc = HiveContext(sc)

rows = hc.sql("select count(distinct(video_duration_group)) from datacubic.app_picserversweibof6vwt_wapvideodownload where log_dir = 20151027150000").collect()

sc.stop()

for row in rows:
    print row
