from pyspark import SparkConf, SparkContext

conf = SparkConf()

conf.setAppName("spark_app_wordcount")

sc = SparkContext(conf=conf)

lines = sc.textFile("/user/hdfs/rawlog/app_weibomobile03x4ts1kl_mwb_interface")

words = lines.flatMap(lambda line: line.split(" "))

words = words.coalesce(30)

pairs = words.map(lambda word: (word, 1))

counts = pairs.reduceByKey(lambda a, b: a + b)

counts.saveAsSequenceFile("/tmp/wordcount")

sc.stop()
