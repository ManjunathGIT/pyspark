from pyspark import SparkConf, SparkContext

conf = SparkConf()

conf.setAppName("spark_app_wordcount_extend")

sc = SparkContext(conf=conf)

lines = sc.newAPIHadoopFile(
    "/user/yurun/spark/textfile/",
    "org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat",
    "org.apache.hadoop.io.Text",
    "org.apache.hadoop.io.LongWritable")

results = lines.collect()

for result in results:
    print result

sc.stop()
