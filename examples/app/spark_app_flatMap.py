from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("spark_app_flatMap")

sc = SparkContext(conf=conf)

rdd = sc.parallelize([1, 2, 3])

datas = rdd.flatMap(lambda val: [val, val + 1, val + 2]).collect()

datas2 = rdd.flatMap(lambda val: [[val], [val + 1], [val + 2]).collect()

sc.stop()

print "datas:", datas

print "datas2:", datas2
