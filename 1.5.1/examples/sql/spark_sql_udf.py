from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql.types import StringType, MapType

conf = SparkConf().setAppName("spark_sql_udf")

sc = SparkContext(conf=conf)

sqlCtx = SQLContext(sc)

lines = sc.parallelize(["a", "b", "c"])

people = lines.map(lambda value: Row(name=value))

peopleSchema = sqlCtx.inferSchema(people)

peopleSchema.registerTempTable("people")


def myfunc(value):
    return value.upper()


def func_map():
    # dictionary
    map = {}

    map["a"] = 1
    map["b"] = 2
    map["c"] = 3

    return map

sqlCtx.registerFunction("myfunc", myfunc, StringType())

rows = sqlCtx.sql("select func_map() from people").collect()

sc.stop()

for row in rows:
    print row
