# coding: utf-8

from pyspark import SparkConf, SparkContext
from pyspark.sql import HiveContext
import re
from pyspark.sql import StructType, StructField, StringType, IntegerType
import json

conf = SparkConf().setAppName("app_picserversweibof6vwt_wapvideodownload")

sc = SparkContext(conf=conf)

hc = HiveContext(sc)

source = sc.textFile(
    "/user/hdfs/rawlog/app_picserversweibof6vwt_wapvideodownload/2015_09_21/00")

pattern = re.compile("^([^`]*)`([^`]*)")


def lineParse(line):
    matcher = pattern.match(line)

    if(matcher):
        return matcher.groups()
    else:
        return None

rows = source.map(lineParse).filter(
    lambda columns: columns and len(columns) == 2)

schema = StructType([StructField("videodownload_info", StringType(
), False), StructField("shostname", StringType(), False)])

table = hc.applySchema(rows, schema)

table.registerTempTable("temp_table")


def json_transform(data):
    try:
        data = json.loads(data)

        if 'ua' not in data:
            pass
        elif len(data['ua'].split('__')) < 3:
            pass
        elif data['ua'].split('__')[2] == '5.4.0':
            if 'video_buffer_type' not in data:
                pass
            elif data['video_buffer_type'] == "1":
                data['buffer_duration_list'] = [
                    round(float(data['video_buffer_duration']))]
            elif data['video_buffer_type'] != "1":
                data['video_play_type'] = data['video_buffer_type']

                if 'video_buffer_duration' in data:
                    data['video_play_type_duration'] = data[
                        'video_buffer_duration']
        elif data['ua'].split('__')[2] == '5.4.5' or data['ua'].split('__')[2] == '5.4.5_beta':
            buffer_duration_list = []

            if 'video_time_duration' not in data:
                pass
            else:
                for t in data['video_time_duration']:
                    if t['type'] == '1':
                        buffer_duration_list.append(
                            round(float(t['duration'])))
                    elif t['type'] != '1':
                        data['video_play_type'] = t['type']
                        data['video_play_type_duration'] = t['duration']

            if 'video_error_info' not in data:
                pass
            elif 'video_error_info' in data:
                data['video_error_code'] = data[
                    'video_error_info']['error_code']
                data['video_error_msg'] = data['video_error_info']['error_msg']

            data['buffer_duration_list'] = buffer_duration_list
        elif data['ua'].split('__')[2] < 5.4:
            pass

        return json.dumps(data)
    except Exception, e:
        pass

hc.registerFunction("json_transform", json_transform)

result = hc.sql(
    "select json_transform(videodownload_info) as item from temp_table")

result.registerTempTable("temp_table2")

result = hc.sql("""
select 
    get_json_object(item,"$.__date")  as  cdate,
    get_json_object(item,"$.video_mediaid")  as video_mediaid,
    get_json_object(item,"$.video_url") as video_url,
    get_json_object(item,"$.ua") as ua,
    get_json_object(item,"$.video_cdn") as video_cdn,
    get_json_object(item,"$.video_network") as video_network,
    get_json_object(item,"$.ip") as ip,
    get_json_object(item,"$.video_play_type") as video_play_type ,
    get_json_object(item,"$.video_play_type_duration") as video_play_type_duration,
    get_json_object(item,"$.video_error_code") as video_error_code,
    get_json_object(item,"$.video_error_msg") as video_error_msg,
    get_json_object(item,"$.buffer_duration_list") as buffer_duration_list,
    get_json_object(item,"$.video_duration") as video_duration,
    get_json_object(item,"$.video_play_duration") as video_play_duration
from temp_table2
""")

result.registerTempTable("temp_table3")


def str_to_list(str):
    list = []

    str = str[1:(len(str) - 1)]

    temp = str.split(",")

    for v in temp:
        list.append(v.strip())

    return list


def cal_buffer_num(set):
    buffer_count = 0
    buffer_t_sum = 0
    buffer_smaller_500ms_count = 0
    buffer_bigger_2min_count = 0

    if set == None:
        pass
    else:
        list = str_to_list(set)

        for s in list:
            if s >= 500 and s <= 120000:
                buffer_count = buffer_count + 1
                buffer_t_sum = buffer_t_sum + s
            elif s < 500:
                buffer_smaller_500ms_count = buffer_smaller_500ms_count + 1
            elif s > 120000:
                buffer_bigger_2min_count = buffer_bigger_2min_count + 1

    return (buffer_count, buffer_t_sum, buffer_smaller_500ms_count, buffer_bigger_2min_count)

hc.registerFunction("cal_buffer_num", cal_buffer_num, StructType([StructField("buffer_count", IntegerType()), StructField(
    "buffer_t_sum", IntegerType()), StructField("buffer_smaller_500ms_count", IntegerType()), StructField("buffer_bigger_2min_count", IntegerType())]))

result = hc.sql("""
select 
	from_unixtime(cast(round(cdate,0) as bigint),'yyyy-MM-dd') as date,
	(
		case
			when func.ipToLocationBySina(ip)[0]='中国' then func.ipToLocationBySina(ip)[1]
			when func.ipToLocationBySina(ip)[0]!='中国' then func.ipToLocationBySina(ip)[0]
		end
	) as province,
	func.ipToLocationBySina(ip)[4] as isp,
	(
		case
			when video_cdn like 'f=cnct%' then 'cnct'
			when video_cdn like 'f=alicdn%' then 'ali'
			when video_cdn like 'f=edge%' then 'edge'
			else 'else'
		END
	) as cdn,
	split(split(video_cdn,'s=')[1],',')[0] as idc,
	(
		CASE
			WHEN upper(ua) LIKE '%IPHONE%' THEN 'IPHONE'
			WHEN upper(ua) LIKE '%ANDROID%' THEN 'ANDROID'
			ELSE '-'
		END
	) AS ua,
	split(ua, '__')[2] as version,
	video_network,
	video_error_code,
	video_error_msg,
	video_play_duration,
	video_duration,
	(
		case
        	when video_play_duration>0 and video_duration>0 then cast(round(video_play_duration/video_duration,1) as VARCHAR(5))
        	when video_play_duration='' or video_play_duration=0 then 'NoPlay'
        	else '-'
        END
    ) as play_process_group,
	video_play_duration/video_duration as play_process,
	(
		case
			when video_play_duration>0 then video_play_type_duration
			else '' 
		end
	) as video_init_duration,
	(
		case
			when video_play_duration>0 and video_play_type_duration <=2000 then '<=2000ms'
			when video_play_duration>0 and video_play_type_duration>2000 then '>2000ms' else '-'
		end
	) as init_timetag,
	cal_buffer_num(buffer_duration_list) as cal_buffer_num
from temp_table3 where (video_url like '%%us.sina%' or video_mediaid like '1034:%') and split(ua, '__')[2] > '5.3'
""")

result.registerTempTable("temp_table4")

result = hc.sql("""
select 
	date,province,isp,cdn,idc,ua,version,video_network, video_error_code,video_error_msg,
	init_timetag,cal_buffer_num.buffer_count,cal_buffer_num.buffer_smaller_500ms_count,cal_buffer_num.buffer_bigger_2min_count,
	play_process_group,
	sum(video_init_duration) as sum_video_init_duration,sum(cal_buffer_num.buffer_t_sum) as sum_buffer_t_sum
from temp_table4
""").take(10)

sc.stop()

for row in result:
    print row
