from pyspark import SparkConf, SparkContext

conf = SparkConf()

conf.setAppName("spark_app_wordcount_extend")

sc = SparkContext(conf=conf)

lines = sc.newAPIHadoopFile(
    "/user/yurun/spark/textfile/",
    "org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat",
    "org.apache.hadoop.io.LongWritable",
    "org.apache.hadoop.io.Text")

words = lines.flatMap(lambda line: line.split("\t"))

pairs = words.map(lambda word: (word, 1))

counts = pairs.reduceByKey(lambda a, b: a + b)

results = counts.collect()

for result in results:
    print result

sc.stop()
