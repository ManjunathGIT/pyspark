from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("spark_app_save_data_to_rcfile")

sc = SparkContext(conf=conf)

lines = ["1\t2\t3", "4\t5\t6", "7\t8\t9"]

pairRDD = sc.parallelize(lines).map(lambda line: (None, line))

pairRDD.saveAsHadoopFile(path="hdfs://dip.cdh5.dev:8020/user/yurun/rcfile",
                         outputFormatClass="org.apache.hadoop.hive.ql.io.RCFileOutputFormat",
                         keyClass="org.apache.hadoop.io.NullWritable",
                         valueClass="org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable",
                         valueConverter="com.sina.dip.spark.converter.StringToBytesRefArrayWritableConverter")

sc.stop()
