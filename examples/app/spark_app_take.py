from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("spark_app_take")

sc = SparkContext(conf=conf)

data = sc.parallelize([1, 2, 3]).take(2)

# ValueError: RDD is empty
#data2 = sc.parallelize([]).first()

sc.stop()

print data
