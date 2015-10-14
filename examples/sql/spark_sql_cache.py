# coding=utf-8

from pyspark import SparkConf, SparkContext
from pyspark.sql import HiveContext

conf = SparkConf().setAppName("spark_sql_cache")

sc = SparkContext(conf=conf)

hc = HiveContext(sc)

source = sc.parallelize(
    ['{"col1": "row1_col1","col2":"row1_col2","col3":"row1_col3"}', '{"col1": "row2_col1","col2":"row2_col2","col3":"row2_col3"}', '{"col1": "row3_col1","col2":"row3_col2","col3":"row3_col3"}'])


sourceRDD = hc.jsonRDD(source)

sourceRDD.registerTempTable("temp_source")


def convert(row):
    mydict = row.asDict()

    mydict["col1"] = mydict["col1"].upper()

    return Row(mydict)

mytable = hc.sql(
    "select col1, col2, col3 from temp_source").map(convert)

mytable.registerTempTable("temp_mytable")

# hc.cacheTable("temp_mytable")

datas = hc.sql("select * from temp_mytable").collect()

datas = hc.sql("select col1 from temp_mytable").collect()

# hc.uncacheTable("temp_mytable")

sc.stop()

for data in datas:
    print data


tuple()