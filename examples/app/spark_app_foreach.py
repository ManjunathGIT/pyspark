from pyspark import SparkConf, SparkContext
import logging

conf = SparkConf().setAppName("spark_app_foreach")

sc = SparkContext(conf=conf)


def log(val):
    logging.info("val: " + val)

sc.parallelize(["a", "b", "c"]).foreach(log)

sc.stop()
