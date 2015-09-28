from pyspark import SparkConf, SparkContext, StorageLevel

conf = SparkConf().setAppName("spark_app_pipe")

sc = SparkContext(conf=conf)

datas = sc.parallelize(["1", "2", "3", "4", "5"]).pipe("cat").collect()

sc.stop()

print datas
