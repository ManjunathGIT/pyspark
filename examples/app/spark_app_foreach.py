from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("spark_app_foreach")

sc = SparkContext(conf=conf)

sc.parallelize(["a", "b", "c"]).foreach(lambda val: print val,)

sc.stop()
