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

        if "ua" not in jsonObj:
            return None

        ua = jsonObj["ua"]

        if len(ua.split("__")) < 3:
            return None

        version = ua.split("__")[2]

        if version == "5.4.0" or version == "5.4.5" or version == "5.4.5_beta":
            __date = jsonObj["__date"] if "__date" in jsonObj else ""
            video_mediaid = jsonObj[
                "video_mediaid"] if "video_mediaid" in jsonObj else ""
            video_url = jsonObj["video_url"] if "video_url" in jsonObj else ""
            video_cdn = jsonObj["video_cdh"] if "video_cdh" in jsonObj else ""
            video_network = jsonObj[
                "video_network"] if "video_network" in jsonObj else ""
            ip = jsonObj["ip"] if "ip" in jsonObj else ""
            video_play_type = jsonObj[
                "video_play_type"] if "video_play_type" in jsonObj else ""
            video_play_type_duration = jsonObj[
                "video_play_type_duration"] if "video_play_type_duration" in jsonObj else ""
            video_error_code = jsonObj[
                "video_error_code"] if "video_error_code" in jsonObj else ""
            video_error_msg = jsonObj[
                "video_error_msg"] if "video_error_msg" in jsonObj else ""
            buffer_duration_list = jsonObj[
                "buffer_duration_list"] if "buffer_duration_list" in jsonObj else ""
            video_duration = jsonObj[
                "video_duration"] if "video_duration" in jsonObj else ""
            video_play_duration = jsonObj[
                "video_play_duration"] if "video_play_duration" else ""

        return (__date, video_mediaid, video_url, ua, video_cdn, video_network, ip, video_play_type, video_play_type_duration, video_error_code, video_error_msg, buffer_duration_list, video_duration, video_play_duration)
    except Exception, e:
        pass

    return None

rows = source.map(lineParse).filter(lambda columns: columns)

schema = StructType([StructField("__date", StringType(), False), StructField(
    "video_mediaid", StringType(), False), StructField("video_url", StringType(), False), StructField("ua", StringType(), False), StructField("video_cdh", StringType(), False), StructField("video_network", StringType(), False), StructField("ip", StringType(), False), StructField("video_play_type", StringType(), False), StructField("video_play_type_duration", StringType(), False), StructField("video_error_code", StringType(), False), StructField("video_error_msg", StringType(), False), StructField("buffer_duration_list", StringType(), False), StructField("video_duration", StringType(), False), StructField("video_play_duration", StringType(), False)])

table = hc.applySchema(rows, schema)

table.registerTempTable("temp_table")

result = hc.sql("select * from temp_table").collect()

sc.stop()

for row in result:
    print row
