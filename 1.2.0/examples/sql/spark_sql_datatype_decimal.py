# coding: utf-8

"""Spark SQL DataType

ByteType: int
ShortType: int
IntegerType: int
LongType: long
FloatType: float
DoubleType: float
Decimal: Decimal
StringType: ""
BinaryType: ignore
BooleanType: bool
TimestampType: datetime
DateType: date
ArrayType: list
MapType: dict
StructType: tuple
"""

from pyspark import SparkConf, SparkContext
from pyspark.sql import HiveContext
from decimal import Decimal
from pyspark.sql import StructType, StructField, DecimalType

conf = SparkConf().setAppName("spark_sql_datatype_decimal")

sc = SparkContext(conf=conf)

hc = HiveContext(sc)

source = sc.parallelize(
    [(Decimal("1.0"), Decimal("2.0"))])

schema = StructType([StructField("col1", DecimalType(), False),
                     StructField("col2", DecimalType(), False)])

table = hc.applySchema(source, schema)

table.registerAsTable("temp_table")

rows = hc.sql(
    "select col1, col2 from temp_table").collect()

sc.stop()

for row in rows:
    print row
