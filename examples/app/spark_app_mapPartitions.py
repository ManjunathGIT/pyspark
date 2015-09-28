from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("spark_app_mapPartitions")

sc = SparkContext(conf=conf)

datas = sc.parallelize([1, 2, 3, 4, 5], 3).mapPartitions(
    lambda iterator: sum(iterator)).collect()

sc.stop()

print datas
