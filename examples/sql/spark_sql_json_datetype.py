# coding: utf-8

from pyspark import SparkConf, SparkContext
from pyspark.sql import HiveContext

conf = SparkConf().setAppName("spark_sql_json_datetime")

sc = SparkContext(conf=conf)

hc = HiveContext(sc)

"""
source = sc.parallelize(['{"key1" : 1, "key2" : "2"}'])

jsonRDD = hc.jsonRDD(source)

jsonRDD.registerTempTable("temp_table")

values = hc.sql("select key1, key2 from temp_table").collect()
"""

"""
source = sc.parallelize(['{"key" : {"key1" : 1, "key2" : "2"}}'])

jsonRDD = hc.jsonRDD(source)

jsonRDD.registerTempTable("temp_table")

values = hc.sql("select key.key1, key.key2 from temp_table").collect()
"""

"""
source = sc.parallelize(['{"key" : [1, 2, 3.0]}'])

jsonRDD = hc.jsonRDD(source)

jsonRDD.registerTempTable("temp_table")

values = hc.sql("select key[0], key[1], key[2] from temp_table").collect()
"""

"""
source = sc.parallelize(['{"key" : [1, "2" , 3.0]}'])

jsonRDD = hc.jsonRDD(source)

jsonRDD.registerTempTable("temp_table")

values = hc.sql("select key[0], key[1], key[2] from temp_table").collect()

# values的输出结果：Row(_c0=u'1', _c1=u'2', _c2=u'3.0')，数据类型被全部推断为“int”
"""

source = sc.parallelize(['{"key" : [1, "2" , 3.0]}'])

jsonRDD = hc.jsonRDD(source)

jsonRDD.registerTempTable("temp_table")

values = hc.sql("select key from temp_table").collect()

sc.stop()

for value in values:
    print value
