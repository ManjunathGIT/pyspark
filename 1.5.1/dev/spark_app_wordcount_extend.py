from pyspark import SparkConf, SparkContext

conf = SparkConf()

conf.setAppName("spark_app_wordcount_extend")

sc = SparkContext(conf=conf)

lines = sc.newAPIHadoopFile(
    "/user/yurun/spark/textfile/",
    "org.apache.hadoop.mapreduce.lib.input.TextInputFormat",
    "org.apache.hadoop.io.LongWritable",
    "org.apache.hadoop.io.Text")

results = lines.collect()

for result in results:
    print result

sc.stop()
