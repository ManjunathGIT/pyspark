from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("spark_app_groupByKey")

sc = SparkContext(conf=conf)

datas = sc.parallelize(
    [("a", 1), ("b", 1), ("b", 2), ("c", 1), ("c", 2), ("c", 3)]).groupByKey().collect()

sc.stop()

print[(x, list(y)) for x, y in datas]
