from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("spark_app_count")

pairs = conf.getAll()

for pair in pairs:
	print pair[0],pair[1]

sc = SparkContext(conf=conf)

data = sc.parallelize([1, 2, 3, 4, 5]).count()

sc.stop()


print "data:", data
