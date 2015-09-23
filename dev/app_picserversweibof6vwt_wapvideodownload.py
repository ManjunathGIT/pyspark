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

    if not matcher:
        return None

    videodownload_info = matcher.group(1)

    try:
        jsonObj = json.loads(videodownload_info)

        if "__date" not in jsonObj or "video_mediaid" not in jsonObj or "video_url" not in jsonObj or "ua" not in jsonObj or "video_cdn" not in jsonObj or "video_network" not in jsonObj or "ip" not in jsonObj or "video_play_type" not in jsonObj or "video_play_type_duration" not in jsonObj or "video_error_code" not in jsonObj or "video_error_msg" not in jsonObj or "buffer_duration_list" not in jsonObj or "video_duration" not in jsonObj or "video_play_duration" not in jsonObj:
            pass

        return videodownload_info
    except Exception, e:
        pass

    return None

rows = source.map(lineParse).filter(lambda columns: columns).collect()

sc.stop()

for row in rows:
    print row
