from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("spark_app_mapPartitions")

sc = SparkContext(conf=conf)


def f(vals):
    return sum(vals)

datas = sc.parallelize([1, 2, 3, 4, 5, 6], 3).mapPartitions(f).collect()

sc.stop()

print datas
