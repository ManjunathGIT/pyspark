from pyspark import SparkConf, SparkContext
from pyspark.sql import HiveContext, Row, StructType, StructField, StringType

conf = SparkConf().setAppName("spark_app_accumulator")

sc = SparkContext(conf=conf)

hc = HiveContext(sc)

allLines = sc.accumulator(0)
successLines = sc.accumulator(0)
errorLines = sc.accumulator(0)

source = sc.parallelize(["col1 col2 col3" for index in range(100)])


def lineFilter(columns):
    allLines.add(1)

    if columns and len(columns) == 3:
        successLines.add(1)

        return True
    else:
        errorLines.add(1)

        return False

columns = source.map(lambda line: line.split(" ")).filter(lineFilter)

rows = columns.map(
    lambda columns: (columns[0], columns[1], columns[2]))

schema = StructType([StructField("col1", StringType()), StructField(
    "col2", StringType()), StructField("col3", StringType())])

table = hc.applySchema(rows, schema)

table.registerAsTable("temp_mytable")

datas = hc.sql("select * from temp_mytable").collect()

sc.stop()

if datas:
    for data in datas:
        print data

print "allLines:", allLines.value
print "successLines:", successLines.value
print "errorLines: ", errorLines.value
