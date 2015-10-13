from pyspark import SparkConf, SparkContext
from pyspark.sql import HiveContext

conf = SparkConf().setAppName("xinqi_group")

sc = SparkContext(conf=conf)

hc = HiveContext(sc)

spark_sql = '''select job_date,cdn,province,isp,ua,idc,play_process_group,version,init_timetag,buffer_count,
             sum(sum_play_process) as sum_play_process,
             sum(sum_video_init_duration) as sum_video_init_duration,
             sum(sum_buffer_t_sum) as sum_buffer_t_sum,
             sum(num) as num
             from(
             select job_date,cdn,province,isp,ua,play_process_group,version,init_timetag,buffer_count,sum_play_process,sum_video_init_duration,sum_buffer_t_sum,num,
             temp_split_idc(idc) as idc
             from datacubic.app_picserversweibof6vwt_wapvideodownload
             where log_dir>='20151012000000' and log_dir<='20151012230000' and version>='5.4.5'
             )a
             group by job_date,cdn,province,isp,ua,idc,play_process_group,version,init_timetag,buffer_count'''

spark_sql = """
select job_date,cdn,province,isp,ua,play_process_group,version,
	init_timetag,buffer_count,sum_play_process,sum_video_init_duration,
	sum_buffer_t_sum,num,idc 
from datacubic.app_picserversweibof6vwt_wapvideodownload
where version>='5.4.5' and log_dir>='20151012000000' and log_dir<='20151012230000' group by job_date,cdn,province,isp,ua,idc,play_process_group,version,init_timetag,buffer_count
"""


def noneFilter(row):
    for column in row:
        if column == None:
            return True

    return False

rows = hc.sql(spark_sql).count()

sc.stop()

print rows
