from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("spark_app_foreachPartition")

sc = SparkContext(conf=conf)


def log(iterator):
    for val in iterator:
        print val

sc.parallelize([1, 2, 3, 4, 5]).coalesce(3).foreachPartition(log)

sc.stop()
