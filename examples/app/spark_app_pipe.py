from pyspark import SparkConf, SparkContext, StorageLevel

conf = SparkConf().setAppName("spark_app_pipe")

sc = SparkContext(conf=conf)

datas = sc.parallelize(
    ["/tmp/1", "/tmp/2", "/tmp/3", "/tmp/4", "/tmp/5"]).pipe("mkdir").collect()

sc.stop()

print datas
