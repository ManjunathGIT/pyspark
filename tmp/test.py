from pyspark import SparkConf, SparkContext
from pyspark.sql import HiveContext, Row

conf = SparkConf().setAppName("spark_sql_delimiter_infer_schema")

sc = SparkContext(conf=conf)

hc = HiveContext(sc)

source = sc.parallelize(["row1_col1?row1_col2row1_col3"])

rows = source.map(
    lambda columns: Row(col1=columns))

table = hc.inferSchema(rows)

table.registerAsTable("temp_mytable")

datas = hc.sql("select split(col1,'\\\\?') from temp_mytable").collect()

sc.stop()

if datas:
    for data in datas:
        print data
