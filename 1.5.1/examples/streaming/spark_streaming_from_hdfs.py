from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext

conf = SparkConf().setAppName("spark_streaming_from_hdfs")

sc = SparkContext(conf=conf)

streamingCtx = StreamingContext(sc, 10)

filePathDStream = streamingCtx.textFileStream(
    "hdfs://dip.cdh5.dev:8020/user/yurun/data/")


def convertRDD(filePathRDD):
    return filePathRDD.map(lambda filePath: filePathRDD.context.textFile(filePath)).reduce(
        lambda rddA, rddB: rddA.union(rddB))

fileLineDStream = filePathDStream.transform(
    lambda filePathRDD: convertRDD(filePathRDD))

wordcounts = fileLineDStream.flatMap(
    lambda line: line.split(" ")).map(lambda word: (word, 1)).reduceByKey(lambda countA, countB: countA + countB)

wordcounts.pprint()

streamingCtx.start()

streamingCtx.awaitTermination()
