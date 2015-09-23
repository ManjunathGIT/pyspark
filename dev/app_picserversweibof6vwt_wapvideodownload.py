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

        __date = None
        video_mediaid = None
        video_url = None
        ua = None
        video_cdn = None
        video_network = None
        ip = None
        video_play_type = None
        video_play_type_duration = None
        video_error_code = None
        video_error_msg = None
        buffer_duration_list = None
        video_duration = None
        video_play_duration = None

        ua = jsonObj["ua"]

        if len(split(ua, "__")) < 3:
            pass

        version = ua.split("__")[2]

        if version == "5.4.0" or version == "5.4.5" or version == "5.4.5_beta":
            __date = jsonObj["__date"]
            video_mediaid = jsonObj["video_mediaid"]
            video_url = jsonObj["video_url"]
            video_cdn = jsonObj["video_cdn"]
            video_network = jsonObj["video_network"]
            ip = jsonObj["ip"]
            video_error_code = jsonObj["video_error_code"]
            video_error_msg = jsonObj["video_error_msg"]
            video_duration = jsonObj["video_duration"]
            video_play_duration = jsonObj["video_play_duration"]

            if version == "5.4.0":
                pass
            else:
                pass
        else:
            pass

        return (__date, video_mediaid, video_url, ua, video_cdn, video_network, ip, video_play_type, video_play_type_duration, video_error_code, video_error_msg, buffer_duration_list, video_duration, video_play_duration)
    except Exception, e:
        pass

    return None

rows = source.map(lineParse).filter(lambda columns: columns).collect()

sc.stop()

for row in rows:
    print row
