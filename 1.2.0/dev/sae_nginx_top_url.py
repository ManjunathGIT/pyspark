from pyspark import SparkConf, SparkContext
from pyspark.sql import HiveContext
import MySQLdb

conf = SparkConf().setAppName("sae_nginx_top_url")

sc = SparkContext(conf=conf)


def mysqldb(host, port, user, passwd, db, sql):
    conn = None

    cur = None

    try:
        conn = MySQLdb.connect(host=host, port=port, user=user,
                               passwd=passwd, db=db, charset="utf8")

        cur = conn.cursor()

        cur.execute(sql)

        return cur.fetchall()
    except Exception, e:
        pass
    finally:
        if cur:
            try:
                cur.close()
            except Exception, e:
                pass

        if conn:
            try:
                conn.close()
            except Exception, e:
                pass

top_domain_list = mysqldb("m3353i.apollo.grid.sina.com.cn", 3353, "data_history", "f3u4w8n7b3h", "sae",
                          "select domain from (select domain,round(sum(flow)/1024/1024,0)  as flow_MB     from sae.sae_nginx_flow flow    where DATE_SUB(CURDATE(), INTERVAL 1 DAY) = date group by domain order by flow_MB desc limit 20)A")

print top_domain_list

sc.stop()
