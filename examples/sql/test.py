from pyspark.sql import Row

mydict = {"key1": "value1"}

row = Row(mydict)

print row.__FIELDS__