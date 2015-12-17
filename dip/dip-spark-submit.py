import sys
import commands

if len(sys.argv) == 1:
    print "dip-spark-submit.py version hadoopConfDir sparkConfDir sparkParams"

version = sys.argv[1]

home = ""

versions = ["1.5.1"]

if version in versions:
    if version == "1.5.1":
        home = "/usr/lib/spark-1.5.1-bin-2.5.0-cdh5.3.2"
else:
    print "version must be [" + ",".join(versions) + "]"

    return

hadoopConf = sys.argv[2]

sparkConf = sys.argv[3]

cmd = "export HADOOP_CONF_DIR=%s;" % hadoopConf

cmd += "export SPARK_CONF_DIR=%s;" % sparkConf

cmd += "export PYTHONHASHSEED=0;"

cmd += ("%s/bin/spark-class org.apache.spark.deploy.SparkSubmit " %
        home + " ".join(sys.argv[4:]))

output = commands.getoutput(cmd)

print output
