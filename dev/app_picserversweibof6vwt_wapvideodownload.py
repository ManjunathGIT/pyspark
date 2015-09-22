from pyspark import SparkConf, SparkContext
from pyspark.sql import HiveContext
import re
from pyspark.sql import StructType, StructField, StringType
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
    "select json_transform(videodownload_info) as item from temp_table").take(10)

sc.stop()

for row in result:
    print row
