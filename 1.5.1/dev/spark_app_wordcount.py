from pyspark import SparkConf, SparkContext

conf = SparkConf()

conf.setAppName("spark_app_wordcount")

sc = SparkContext(conf=conf)

lines = sc.textFile("/user/yurun/spark/textfile/")

results = lines.collect()

for result in results:
    print result

sc.stop()
