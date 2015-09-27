from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("spark_app_foreach")

sc = SparkContext(conf=conf)


def output(val):
    print val,

sc.parallelize(["a", "b", "c"]).foreach(output)

sc.stop()
