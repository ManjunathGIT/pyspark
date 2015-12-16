import time

ISOTIMEFORMAT = '%Y%m%d%H%M%S'

print time.strftime(ISOTIMEFORMAT, time.localtime())

import commands

path = "/usr/home/yurun/wordcount.jar"

classname = "com.weibo.dip.mr.WordCountExampleMain"

jobname = "wordcount_example"

reduces = "3"

queue = "dip"

commands.getstatusoutput("java -cp %s:/etc/hadoop/conf:/usr/lib/hadoop/lib/*:/usr/lib/hadoop/.//*:/usr/lib/hadoop-hdfs/./:/usr/lib/hadoop-hdfs/lib/*:/usr/lib/hadoop-hdfs/.//*:/usr/lib/hadoop-yarn/lib/*:/usr/lib/hadoop-yarn/.//*:/usr/lib/hadoop-mapreduce/lib/*:/usr/lib/hadoop-mapreduce/.//* %s -D mapreduce.job.name=%s -D mapreduce.job.reduces=%s -D mapreduce.job.queuename=%s" % (path, classname, jobname, reduces, queue))
