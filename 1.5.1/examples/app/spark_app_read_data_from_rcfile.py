from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("spark_app_read_data_from_rcfile")

sc = SparkContext(conf=conf)

lineRDD = sc.hadoopFile(path="hdfs://dip.cdh5.dev:8020/user/yurun/rcfile",
                        inputFormatClass="org.apache.hadoop.hive.ql.io.RCFileInputFormat",
                        keyClass="org.apache.hadoop.io.NullWritable",
                        valueClass="org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable",
                        valueConverter="com.sina.dip.spark.converter.BytesRefArrayWritableToStringConverter").map(lambda pair: pair[1])

lines = lineRDD.collect()

for line in lines:
    print line

sc.stop()
