from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("spark_app_keyBy")

sc = SparkContext(conf=conf)

datas = sc.parallelize([1, 2, 3, 4, 5]).keyBy(lambda val: val ** 3).collect()

sc.stop()

print datas
