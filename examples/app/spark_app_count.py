from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("spark_app_count")

sc = SparkContext(conf=conf)

data = sc.parallelize([1, 2, 3, 4, 5]).count()

sc.stop()


print "data:", data
