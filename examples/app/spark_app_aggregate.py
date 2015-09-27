from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("spark_app_aggregate")

sc = SparkContext(conf=conf)

seqOp = (lambda x, y: x + y)

combOp = (lambda x, y: x + y)

data = sc.parallelize([1, 2, 3, 4]).aggregate(0, seqOp, combOp)

sc.stop()

print data
