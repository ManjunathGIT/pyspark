from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("spark_app_takeOrdered")

sc = SparkContext(conf=conf)

data = sc.parallelize([("c", 3), ("b", 2), ("a", 1)]).takeOrdered(
    2, key=lambda val: val[1])

# ValueError: RDD is empty
#data2 = sc.parallelize([]).first()

sc.stop()

print data
