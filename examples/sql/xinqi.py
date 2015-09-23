from pyspark import SparkConf, SparkContext
from pyspark.sql import HiveContext, StructType, StructField, StringType, DoubleType

conf = SparkConf().setAppName("spark_sql_delimiter_specify_schema")

sc = SparkContext(conf=conf)

hc = HiveContext(sc)

source = sc.parallelize(["1.23 1.23 1.23"])

columns = source.map(lambda line: line.split(" ")).filter(
    lambda columns: columns and len(columns) == 3)

rows = columns.map(
    lambda columns: (float(columns[0]), float(columns[1]), float(columns[2]))

schema=StructType([StructField("col1", DoubleType(), False), StructField(
    "col2", DoubleType(), False), StructField("col3", DoubleType(), False)])

table=hc.applySchema(rows, schema)

table.registerAsTable("temp_mytable")

datas=hc.sql(
    "select cast(col1 as VARCHAR(5)),sum(col2) from temp_mytable").collect()

sc.stop()

if datas:
    for data in datas:
        print data
