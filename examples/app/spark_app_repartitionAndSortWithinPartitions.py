from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("spark_app_repartitionAndSortWithinPartitions")

sc = SparkContext(conf=conf)

datas = sc.parallelize([(1, "a"), (2, "b"), (3, "c"), (4, "d"), (5, "e")]).repartitionAndSortWithinPartitions(
    numPartitions=2).glom().collect()

sc.stop()

print datas
