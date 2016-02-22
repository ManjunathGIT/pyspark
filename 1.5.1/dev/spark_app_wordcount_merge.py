from pyspark import SparkConf, SparkContext

conf = SparkConf()

conf.setAppName("spark_app_wordcount_merge")

sc = SparkContext(conf=conf)

hadoopConf = {"mapreduce.input.fileinputformat.inputdir": "/user/hdfs/rawlog/app_weibomobilekafka1234_topweiboimpression",
              "mapreduce.input.fileinputformat.input.dir.recursive": "true"}

source = sc.newAPIHadoopRDD(inputFormatClass="org.apache.hadoop.mapreduce.lib.input.TextInputFormat",
                            keyClass="org.apache.hadoop.io.LongWritable",
                            valueClass="org.apache.hadoop.io.Text",
                            conf=hadoopConf)

source = source.coalesce(1000)

lines = source.map(lambda pair: pair[1])

words = lines.flatMap(lambda line: line.split(" "))

pairs = words.map(lambda word: (word, 1))

counts = pairs.reduceByKey(lambda a, b: a + b, 50)

counts.saveAsTextFile("/user/yurun/spark/output/wordcount/")

sc.stop()
