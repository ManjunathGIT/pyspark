from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("spark_app_sum")

sc = SparkContext(conf=conf)

datas = sc.parallelize([(1, 1), (2, 2), (3, 3), (4, 4), (5, 5)]).partitionBy(
    2, lambda val: val).glom().collect()
sc.stop()

#
print datas
