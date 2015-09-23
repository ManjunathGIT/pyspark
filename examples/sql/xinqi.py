from pyspark import SparkConf, SparkContext
from pyspark.sql import HiveContext, StructType, StructField, StringType

conf = SparkConf().setAppName("spark_sql_delimiter_specify_schema")

sc = SparkContext(conf=conf)

hc = HiveContext(sc)

source = sc.parallelize(["  "])

columns = source.map(lambda line: line.split(" ")).filter(
    lambda columns: columns and len(columns) == 3)

rows = columns.map(
    lambda columns: (columns[0], columns[1], columns[2]))

schema = StructType([StructField("col1", StringType(), False), StructField(
    "col2", StringType(), False), StructField("col3", StringType(), False)])

table = hc.applySchema(rows, schema)

table.registerAsTable("temp_mytable")

datas = hc.sql("select sum(None) from temp_mytable").collect()

sc.stop()

if datas:
    for data in datas:
        print data
