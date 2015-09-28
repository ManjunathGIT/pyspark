from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("spark_app_union")

sc = SparkContext(conf=conf)

rdd1 = sc.parallelize([1, 2, 3, 4, 5])

rdd2 = sc.parallelize([1, 2, 3, 4, 5])

datas = rdd1.union(rdd2).collect()

sc.stop()

# [5]
print datas
