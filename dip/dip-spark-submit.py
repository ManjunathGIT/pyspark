import sys
import commands

version = sys.argv[1]

home = ""

if version == "1.5.1":
    home = "/usr/lib/spark-1.5.1-bin-2.5.0-cdh5.3.2"

hadoopConf = sys.argv[2]

sparkConf = sys.argv[3]

cmd = ""

cmd.append("export HADOOP_CONF_DIR=%s;" % hadoopConf)

cmd.append("export SPARK_CONF_DIR=%s;" % sparkConf)

cmd.append("export PYTHONHASHSEED=0;")

cmd.append("%s/bin/spark-class org.apache.spark.deploy.SparkSubmit " +
           sys.argv[4:].join(" "))

print cmd
