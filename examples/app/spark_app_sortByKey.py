from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("spark_app_sortByKey")

sc = SparkContext(conf=conf)

datas = sc.parallelize(
    [("c", 1), ("b", 1), ("a", 2), ("a", 1)]).sortByKey().collect()

sc.stop()

print datas
