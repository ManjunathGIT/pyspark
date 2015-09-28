from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("spark_app_mapPartitions")

sc = SparkContext(conf=conf)

datas = sc.parallelize([1, 2, 3, 4, 5, 6], 3).mapPartitions(
    lambda vals: sum(vals)).collect()

sc.stop()

print datas
