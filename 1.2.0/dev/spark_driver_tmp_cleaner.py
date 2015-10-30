import time
import os

clean_endtime = time.time()

print clean_endtime

tmp = "/tmp"

paths = os.listdir(tmp)

for path in paths:
    if os.path.isdir(path) and (path.startswith("spark-") or
                                path.endswith("resources")):
    	spark_clean_dir = os.path.join(tmp, path)

    	print os.path.getctime(spark_clean_dir)
