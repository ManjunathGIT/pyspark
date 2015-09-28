from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("spark_app_sum")

sc = SparkContext(conf=conf)

# only key is passed to paritionFunc
datas = sc.parallelize([1, 2, 3, 4, 5]).map(lambda val: (val, val)).partitionBy(
    2, lambda val: val).map(lambda val: val[0]).glom().collect()

sc.stop()

#
print datas
