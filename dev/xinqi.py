from pyspark import SparkConf, SparkContext
from pyspark.sql import HiveContext

conf = SparkConf().setAppName("xinqi_group")

sc = SparkContext(conf=conf)

hc = HiveContext(sc)

spark_sql = """
select job_date,cdn,province,isp,ua,play_process_group,version,
	init_timetag,buffer_count,sum_play_process,sum_video_init_duration,
	sum_buffer_t_sum,num,temp_split_idc(idc) as idc 
from datacubic.app_picserversweibof6vwt_wapvideodownload
where log_dir>= '%s' and log_dir<='%s' and version>='5.4.5'
"""
rows = hc.sql(spark_sql).filter(lambda row: isinstance(row, tuple)).count()

sc.stop()

print rows
