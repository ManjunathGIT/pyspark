from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("spark_app_aggregate")

sc = SparkContext(conf=conf)

seqOp = (lambda x, y: (x[0] + y, x[1] + 1))

combOp = (lambda x, y: (x[0] + y[0], x[1] + y[1]))

data = sc.parallelize([1, 2, 3, 4]).aggregate((0, 0), seqOp, combOp)

sc.stop()

print data
