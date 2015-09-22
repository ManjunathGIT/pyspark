from pyspark import SparkConf, SparkContext
from pyspark.sql import *
import json

conf = SparkConf().setAppName("spark_sql_udf")

sc = SparkContext(conf=conf)

hc = HiveContext(sc)


def func_array():
    # list
    return [1, 2, 3]

hc.registerFunction("temp_func_array", func_array, ArrayType(LongType()))


def func_struct():
    # tuple
    return (1, 2.0, "3")

hc.registerFunction("temp_func_struct", func_struct, StructType([StructField(
    "a", IntegerType()), StructField("b", FloatType()), StructField("c", StringType())]))


def func_map():
    # dictionary
    map = {}

    map["a"] = 1
    map["b"] = 2
    map["c"] = 3

    return map

hc.registerFunction(
    "temp_func_map", func_map, MapType(StringType(), IntegerType()))

sql_array = """
select item[0], item[1], item[2]
from (
	select temp_func_array() as item from yurun.tablep
) t
"""

sql_struct = """
select item.a, item.b, item.c
from (
	select temp_func_struct() as item from yurun.tablep
) t
"""

sql_map = """
select item["a"], item["b"], item["c"]
from (
	select temp_func_map() as item from yurun.tablep
) t
"""

rows = hc.sql(sql_map).collect()

sc.stop()

for row in rows:
    print row
