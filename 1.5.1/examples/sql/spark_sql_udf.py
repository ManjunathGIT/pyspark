from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, StructType, StructField, IntegerType, LongType, FloatType, StringType, ArrayType, StructType, MapType
import json

conf = SparkConf().setAppName("spark_sql_udf")

sc = SparkContext(conf=conf)

sqlCtx = SQLContext(sc)

source = sc.parallelize([("value",)])

schema = StructType([StructField("col", StringType(), False)])

table = sqlCtx.applySchema(source, schema)

table.registerTempTable("temp_table")


def func_array():
    # list
    return [1, 2, 3]

sqlCtx.registerFunction("temp_func_array", func_array, ArrayType(LongType()))


def func_struct():
    # tuple
    return (1, 2.0, "3")

sqlCtx.registerFunction("temp_func_struct", func_struct, StructType([StructField(
    "a", IntegerType()), StructField("b", FloatType()), StructField("c", StringType())]))


def func_map():
    # dictionary
    map = {}

    map["a"] = 1
    map["b"] = 2
    map["c"] = 3

    return map

sqlCtx.registerFunction(
    "temp_func_map", func_map, MapType(StringType(), IntegerType()))

sql_array = """
select item[0], item[1], item[2]
from (
	select temp_func_array() as item from temp_table
) t
"""

sql_struct = """
select item.a, item.b, item.c
from (
	select temp_func_struct() as item from temp_table
) t
"""

sql_map = """
select item["a"], item["b"], item["c"]
from (
	select temp_func_map() as item from temp_table
) t
"""

rows = sqlCtx.sql(sql_map).collect()

sc.stop()

for row in rows:
    print row
