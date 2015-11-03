datas = {"key1": "val1", "key2": "val2", "key3": "val3"}

print sorted(datas.items())

from pyspark.sql.types import Row

row = Row(key1="val1", key2="val2", key3="val3")

print row.__fields__

print tuple(row)
