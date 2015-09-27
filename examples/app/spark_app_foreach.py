from pyspark import SparkConf, SparkContext
from logging import Logger

conf = SparkConf().setAppName("spark_app_foreach")

sc = SparkContext(conf=conf)


def log(val):
    Logger.info("val: " + val)

sc.parallelize(["a", "b", "c"]).foreach(log)

sc.stop()
