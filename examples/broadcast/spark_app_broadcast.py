# coding: utf-8

from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("spark_app_broadcast")

sc = SparkContext(conf = conf)

sc.stop()