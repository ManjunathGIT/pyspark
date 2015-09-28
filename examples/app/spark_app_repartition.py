from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("spark_app_repartition")

sc = SparkContext(conf=conf)

rdd = sc.parallelize([1, 2, 3], 3)

datas = rdd.glom().collect()

datas2 = rdd.repartition(5).glom().collect()

sc.stop()

print "datas:", datas
print "datas2:", datas2
