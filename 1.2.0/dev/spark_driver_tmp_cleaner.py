import time
import os

now = time.time()

print now

paths = os.listdir("/tmp")

for path in paths:
    # if os.path.isdir(path) and (path.startswith("spark-") or
    # path.endswith("resources")):
    print path, os.path.getmtime(path)
