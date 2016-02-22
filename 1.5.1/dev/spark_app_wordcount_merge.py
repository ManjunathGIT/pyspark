from pyspark import SparkConf, SparkContext

conf = SparkConf()

conf.setAppName("spark_app_merge")

sc = SparkContext(conf=conf)

hadoopConf = {"mapreduce.input.fileinputformat.inputdir": "/user/yurun/spark/textfile/",
              "mapreduce.input.fileinputformat.input.dir.recursive": "true"}

source = sc.newAPIHadoopRDD(inputFormatClass="org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat",
                            keyClass="org.apache.hadoop.io.LongWritable",
                            valueClass="org.apache.hadoop.io.Text",
                            conf=hadoopConf)

lines = source.map(lambda pair: pair[1])

words = lines.flatMap(lambda line: line.split("\t"))

pairs = words.map(lambda word: (word, 1))

counts = pairs.reduceByKey(lambda a, b: a + b)

counts.saveAsTextFile("/user/yurun/spark/output/1")

sc.stop()
