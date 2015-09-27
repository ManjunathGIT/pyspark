from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("spark_app_foreach")

sc = SparkContext(conf=conf)

datas = sc.parallelize(["a", "b", "c"]).foreach(
    lambda val: val.upper()).collect()

sc.stop()

print datas
