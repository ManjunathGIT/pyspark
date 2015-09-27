from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("spark_app_foreach")

sc = SparkContext(conf=conf)

rdd = sc.parallelize(["a", "b", "c"])

rdd.foreach(lambda val: val.upper())

datas = rdd.collect()

sc.stop()

print datas
