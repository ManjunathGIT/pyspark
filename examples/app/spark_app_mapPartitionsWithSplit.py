from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("spark_app_mapPartitionsWithSplit")

sc = SparkContext(conf=conf)


def f(index, vals):
    yield index + sum(vals)

datas = sc.parallelize([1, 2, 3, 4, 5], 3).mapPartitionsWithIndex(f).collect()

sc.stop()

print datas
