from pyspark import SparkConf, SparkContext
from pyspark.sql import HiveContext, Row

conf = SparkConf().setAppName("spark_sql_table_join")

sc = SparkContext(conf=conf)

sqlCtx = HiveContext(sc)

line1 = sc.parallelize(["name1 a", "name3 c", "name4 d"])

line2 = sc.parallelize(["name1 1", "name2 2", "name3 3"])

word1 = line1.map(lambda line: line.split(" "))

word2 = line2.map(lambda line: line.split(" "))

temp_table1 = word1.map(lambda words: Row(name=words[0], title=words[1]))

temp_table2 = word2.map(lambda words: Row(name=words[0], fraction=words[1]))

tableSchema1 = sqlCtx.inferSchema(temp_table1)

tableSchema2 = sqlCtx.inferSchema(temp_table2)

tableSchema1.registerTempTable("temp_table1")

tableSchema2.registerTempTable("temp_table2")


def printRows(rows):
    if rows:
        for row in rows:
            print row

# inner join
rows = sqlCtx.sql(
    "select temp_table1.name, temp_table1.title, temp_table2.fraction from temp_table1 join temp_table2 on temp_table1.name = temp_table2.name").collect()

printRows(rows)

print "============================================="

# left outer join
rows = sqlCtx.sql(
    "select temp_table1.name, temp_table1.title, temp_table2.fraction from temp_table1 left outer join temp_table2 on temp_table1.name = temp_table2.name").collect()

printRows(rows)

# right outer join
rows = sqlCtx.sql(
    "select temp_table1.name, temp_table1.title, temp_table2.fraction from temp_table1 right outer join temp_table2 on temp_table1.name = temp_table2.name").collect()

print "============================================="

printRows(rows)

# full outer join
rows = sqlCtx.sql(
    "select temp_table1.name, temp_table1.title, temp_table2.fraction from temp_table1 full outer join temp_table2 on temp_table1.name = temp_table2.name").collect()

print "============================================="

printRows(rows)

sc.stop()
