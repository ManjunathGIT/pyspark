import time
import os

clean_endtime = time.time()

print clean_endtime

tmp = "/tmp"

paths = os.listdir(tmp)

for path in paths:
	spark_clean_dir = os.path.join(tmp, path)

	if os.path.isdir(spark_clean_dir) and (path.startswith("spark-") or path.endswith("_resources")):
		print os.path.getctime(spark_clean_dir)
