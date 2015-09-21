from pyspark import SparkConf, SparkContext
from pyspark.sql import HiveContext, Row

conf = SparkConf().setAppName("spark_app_accumulator")

sc = SparkContext(conf=conf)

hc = HiveContext(sc)

allLines = sc.accumulator(0)
successLines = sc.accumulator(0)
errorLines = sc.accumulator(0)

source = sc.parallelize(["row1_col1row1_col2 row1_col3",
                         "row2_col1 row2_col2row3_col3", "row3_col1 row3_col2 row3_col3"])


def filter(columns):
    allLines.add(1)

    if columns and len(columns) == 3:
        successLines.add(1)

        return True
    else:
        errorLines.add(1)

        return False

columns = source.map(lambda line: line.split(" ")).filter(filter)

rows = columns.map(
    lambda columns: Row(col1=columns[0], col2=columns[1], col3=columns[2]))

"""
table = hc.inferSchema(rows)

table.registerAsTable("temp_mytable")

datas = hc.sql("select * from temp_mytable").collect()

sc.stop()

if datas:
    for data in datas:
        print data
"""

rows.collect()


print "allLines:", allLines.value
print "successLines:", successLines.value
print "errorLines: ", errorLines.value
