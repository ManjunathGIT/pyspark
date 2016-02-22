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

results = source.collect()

for result in results:
    print result

sc.stop()
