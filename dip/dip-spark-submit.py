import sys
import commands

version = sys.argv[1]

home = ""

if version == "1.5.1":
    home = "/usr/lib/spark-1.5.1-bin-2.5.0-cdh5.3.2"

hadoopConf = sys.argv[2]

sparkConf = sys.argv[3]

cmd = "export HADOOP_CONF_DIR=%s;" % hadoopConf

cmd += "export SPARK_CONF_DIR=%s;" % sparkConf

cmd += "export PYTHONHASHSEED=0;"

cmd += ("%s/bin/spark-class org.apache.spark.deploy.SparkSubmit " %
        " ".join(sys.argv[4:]))

print cmd
