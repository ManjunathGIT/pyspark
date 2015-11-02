from pyspark import SparkConf, SparkContext
from pyspark.sql import HiveContext, StructType, StructField, StringType
import json

conf = SparkConf().setAppName("spark_sql_udf")

sc = SparkContext(conf=conf)

hc = HiveContext(sc)

source = sc.parallelize([("value",)])

schema = StructType([StructField("col", StringType(), False)])

table = hc.applySchema(source, schema)

table.registerTempTable("temp_table")


def func_string():
    return "abc"

hc.registerFunction("func_string", func_string)

rows = hc.sql("select func_string() from temp_table").collect()


def func_int():
    return 123

hc.registerFunction("func_int", func_int)

rows = hc.sql("select func_int() from temp_table").collect()

sc.stop()

for row in rows:
    print row
