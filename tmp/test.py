datas = {"key1": "val1", "key2": "val2", "key3": "val3"}

print sorted(datas.items())

from pyspark.sql.types import Row

row = Row(key1="val1", key2="val2", key3="val3")

print row.__fields__

print tuple(row)

print zip(row.__fields__, tuple(row))

from collections import namedtuple

NamedRow = namedtuple("NamedRow", ["key1", "key2", "key3"])

row = NamedRow(key1="val1", key2="val2", key3="val3")

print row._fields

print tuple(row)

print zip(row._fields, tuple(row))