from pyspark import SparkConf, SparkContext
from pyspark.sql import HiveContext, StructType, StructField, IntegerType, LongType, FloatType, StringType, ArrayType, StructType, MapType
import json

conf = SparkConf().setAppName("spark_sql_udf")

sc = SparkContext(conf=conf)

hc = HiveContext(sc)

source = sc.parallelize([("value",)])

schema = StructType([StructField("col", StringType(), False)])

table = hc.applySchema(source, schema)

table.registerTempTable("temp_table")

rows = hc.sql("select * from temp_table").collect()

sc.stop()

for row in rows:
    print row
