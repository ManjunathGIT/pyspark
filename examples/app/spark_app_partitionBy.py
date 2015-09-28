from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("spark_app_sum")

sc = SparkContext(conf=conf)

datas = sc.parallelize([1, 2, 3, 4, 5]).partitionBy(
    2, lambda val: 0 if val % 2 == 0 else 1).glom().collect()

sc.stop()

#
print datas
