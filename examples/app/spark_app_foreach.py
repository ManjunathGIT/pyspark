from pyspark import SparkConf, SparkContext
import logging

logging.basicConfig(level=logging.ERROR)

conf = SparkConf().setAppName("spark_app_foreach")

sc = SparkContext(conf=conf)


def log(val):
    logging.warning("val: " + val)

sc.parallelize(["a", "b", "c"]).foreach(log)

sc.stop()
