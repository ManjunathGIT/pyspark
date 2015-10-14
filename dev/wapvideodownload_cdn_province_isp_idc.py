# -*- encoding:utf-8 -*-
from pyspark import SparkConf, SparkContext
from pyspark.sql import HiveContext
import numpy as np
from pandas import *
import pandas as pd

conf = SparkConf().setAppName("wapvideodownload_cdn_province_isp_idc")

sc = SparkContext(conf=conf)

hc = HiveContext(sc)

"""
source = sc.parallelize(
    ['{"col1": "row1_col1","col2":"row1_col2","col3":"row1_col3"}', '{"col1": "row2_col1","col2":"row2_col2","col3":"row2_col3"}', '{"col1": "row3_col1","col2":"row3_col2","col3":"row3_col3"}'])


table = hc.jsonRDD(source)

table.registerTempTable("temp_mytable")

hc.cacheTable("temp_mytable")

datas = hc.sql("select * from temp_mytable").collect()

datas = hc.sql("select col1 from temp_mytable").collect()


def split_idc(idc):
    if idc == None or idc == '' or (not isinstance(idc, basestring)):
        return ''
    else:
        words = idc.split('.')
        if len(words) >= 2:
            return words[0] + '.' + words[1]
        else:
            return ''

hc.registerFunction("temp_split_idc", split_idc)

#--------------------------2.0 RDD-----------------------
spark_sql = '''select '1' as job_date,cdn,province,isp,ua,idc,play_process_group,version,init_timetag,buffer_count,
             sum(sum_play_process) as sum_play_process,
             sum(sum_video_init_duration) as sum_video_init_duration,
             sum(sum_buffer_t_sum) as sum_buffer_t_sum,
             sum(num) as num
             from(
             select cdn,province,isp,ua,play_process_group,version,init_timetag,buffer_count,sum_play_process,sum_video_init_duration,sum_buffer_t_sum,num,
             temp_split_idc(idc) as idc
             from datacubic.app_picserversweibof6vwt_wapvideodownload
             where log_dir= '20151012110000' and version>='5.4.5' limit 10
             )a
             group by cdn,province,isp,ua,idc,play_process_group,version,init_timetag,buffer_count'''
rows_rdd = hc.sql(spark_sql)
rows_rdd.registerTempTable("temp_rdd")
hc.cacheTable("temp_rdd")
#--------------------------2.1 播放总请求量-----------------------
spark_sql = '''select job_date,cdn,province,isp,idc,sum(num) as num
             from temp_rdd
             where job_date= '1' and version>='5.4.5'
             group by job_date,cdn,province,isp,idc'''
rows = hc.sql(spark_sql).collect()
#rows =hc.sql(spark_sql)
# print rows.toDebugString()
print 11111111111111111111
#--------------------------2.2 成功播放量-----------------------
# video_play_duration >0 and error_code=' '即play_process_group为数字时的sum(num)
spark_sql = '''select job_date,cdn,province,isp,idc,sum(num) as num
               from temp_rdd
               where job_date= '1' and play_process_group!='NoPlay' and play_process_group!='-' and version>='5.4.5'
               group by job_date,cdn,province,isp,idc'''
rows = hc.sql(spark_sql).collect()
print 222222222222222222222
"""


def split_idc(idc):
    if idc == None or idc == '' or (not isinstance(idc, basestring)):
        return ''
    else:
        words = idc.split('.')
        if len(words) >= 2:
            return words[0] + '.' + words[1]
        else:
            return ''

hc.registerFunction("temp_split_idc", split_idc)

source = hc.sql("""select cdn,province,isp,ua,play_process_group,version,init_timetag,buffer_count,sum_play_process,sum_video_init_duration,sum_buffer_t_sum,num,
             temp_split_idc(idc) as idc
             from datacubic.app_picserversweibof6vwt_wapvideodownload
             where log_dir= '20151012110000' and version>='5.4.5'""")

source.registerTempTable("source_table")

table = hc.sql("""select '1' as job_date,cdn,province,isp,ua,idc,play_process_group,version,init_timetag,buffer_count,
             sum(sum_play_process) as sum_play_process,
             sum(sum_video_init_duration) as sum_video_init_duration,
             sum(sum_buffer_t_sum) as sum_buffer_t_sum,
             sum(num) as num
             from source_table
             group by cdn,province,isp,ua,idc,play_process_group,version,init_timetag,buffer_count""")

table.registerTempTable("temp_mytable")

hc.cacheTable("temp_mytable")

datas = hc.sql("select * from temp_mytable limit 10").collect()

datas = hc.sql("select count(*) from temp_mytable").collect()

sc.stop()
