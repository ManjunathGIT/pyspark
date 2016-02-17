from pyspark import SparkConf, SparkContext

conf = SparkConf()

conf.setAppName("spark_app_wordcount")

sc = SparkContext(conf=conf)

lines = sc.textFile("/user/yurun/spark/textfile/")

words = lines.flatMap(lambda line: line.split("\t"))

pairs = words.map(lambda word: (word, 1))

counts = pairs.reduceByKey(lambda a, b: a + b)

results = counts.collect()

for result in results:
    print result

sc.stop()
