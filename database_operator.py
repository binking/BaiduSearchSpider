#coding=utf-8
import sys, time
from datetime import datetime as dt, timedelta
import MySQLdb as mdb
import traceback
reload(sys)
sys.setdefaultencoding('utf-8')

MYSQL_GONE_ERROR = -100
OUTER_MYSQL = {
    'host': '582af38b773d1.bj.cdb.myqcloud.com',
    'port': 14811,
    'db': 'webcrawler',
    'user': 'web',
    'passwd': "Crawler20161231",
    'charset': 'utf8',
    'connect_timeout': 20,
}
QCLOUD_MYSQL = {
    'host': '10.66.110.147',
    'port': 3306,
    'db': 'webcrawler',
    'user': 'web',
    'passwd': 'Crawler20161231',
    'charset': 'utf8',
    'connect_timeout': 20,
}


def connect_database():
    """
    We can't fail in connect database, which will make the subprocess zoombie
    """
    attempt = 1
    while True:
        seconds = 3*attempt
        try:
            WEBCRAWLER_DB_CONN = mdb.connect(**QCLOUD_MYSQL)
            return WEBCRAWLER_DB_CONN
        except mdb.OperationalError as e:
            print dt.now().strftime("%Y-%m-%d %H:%M:%S"), "Sleep %s seconds cuz we can't connect MySQL..." % seconds
        except Exception as e:
            traceback.print_exc()
            print dt.now().strftime("%Y-%m-%d %H:%M:%S"), "Sleep %s cuz unknown connecting database error." % seconds
        attempt += 1
        print "@"*10, "Connecting database at %d-th time..." % attempt
        time.sleep(seconds)
  

def write_baidu_topic_into_db(conn, topic_info):
    """
    Update two tables: wechatsearchtopic and wechatsearcharticlerelation
    param topic_info(dict): createdate, uri, search_url, search_keyword, urls
    """
    is_succeed = True
    topic_kw = topic_info.get('search_keyword', '')
    topic_uri = topic_info.get('uri', '')
    topic_date = topic_info.get('createdate', '')
    topic_s_url = topic_info.get('search_url', '')
    # top_url = topic_info.get('top_url', '')
    # top_title = topic_info.get('top_title', '')
    date_range = topic_info.get('date_range', '')
    hit_num = topic_info.get('hit_num', -1)

    deprecate_topic = """UPDATE baidusearchtopic SET is_up2date='N'
        WHERE search_keyword=%s AND date_range=%s AND is_up2date='Y'
    """
    may_existed_topic = """UPDATE baidusearchtopic SET is_up2date='Y' 
        WHERE search_date=%s AND search_keyword=%s AND date_range=%s
    """
    insert_new_topic = """
        INSERT INTO baidusearchtopic 
        (uri, search_url, createdate, search_date, search_keyword, date_range, hit_num)
        VALUES ('%s', '%s', '%s', '%s', '%s', '%s', %d)
    """ % (topic_uri, topic_s_url, topic_date, topic_date, topic_kw, date_range, hit_num)
    try:
        # search_date and search_url ensure newsest articles
        cursor = conn.cursor()
        cursor.execute(deprecate_topic, (topic_kw, date_range))
        conn.commit()
        is_existed = cursor.execute(may_existed_topic, 
            (topic_date, topic_kw, date_range))
        if not is_existed:
            print "\nInserting ",
            cursor.execute(insert_new_topic)
            conn.commit()
        print "$"*10, "Write Baidu topic succeeded..."
    except (mdb.ProgrammingError, mdb.OperationalError) as e:
        traceback.print_exc()
        is_succeed = False
        if 'MySQL server has gone away' in str(e):
            print dt.now().strftime("%Y-%m-%d %H:%M:%S"), "MySQL server has gone away"
        elif 'Deadlock found when trying to get lock' in str(e):
            print dt.now().strftime("%Y-%m-%d %H:%M:%S"), "You did not solve dead lock"
        elif 'Lost connection to MySQL server' in str(e):
            print dt.now().strftime("%Y-%m-%d %H:%M:%S"), "Lost connection to MySQL server"
        elif e.args[0] in [1064, 1366]:
            print dt.now().strftime("%Y-%m-%d %H:%M:%S"), "Wrong Tpoic String"
        else:
            print dt.now().strftime("%Y-%m-%d %H:%M:%S"), "Other Program or Operation Errors"
    except Exception as e:
        traceback.print_exc()
        is_succeed = False
        print dt.now().strftime("%Y-%m-%d %H:%M:%S"), "Write topic failed"
    return is_succeed

def read_topics_from_db(conn, start_date, end_date, interval):
    """
    Read unchecked topics from database, return list of topics
    param start_date(str): YYYY-MM-DD
    """
    select_topic_sql = """
        SELECT DISTINCT title FROM topicinfo
        -- WHERE theme LIKE '新浪微博_热门话题%'
        where createdate > '{fr}'
        and createdate < '{to}'
        ORDER BY createdate DESC 
    """
    if interval:
        days_ago = (dt.today() - timedelta(interval)).strftime("%Y-%m-%d")
        next_day = (dt.today() + timedelta(1)).strftime("%Y-%m-%d")
        select_topic = select_topic_sql.format(fr=days_ago, to=next_day)
    else:
        select_topic = select_topic_sql.format(fr=start_date, to=end_date)
    #select_topic = """
    #    SELECT DISTINCT title FROM topicinfo
    #    WHERE theme LIKE '新浪微博_热门话题%'
    #    AND STR_TO_DATE(createdate, "%Y-%m-%d %H:%i:%s") > '2016-11-01'
    #    AND STR_TO_DATE(createdate, "%Y-%m-%d %H:%i:%s") < '2016-11-10'
    #    ORDER BY STR_TO_DATE(createdate, "%Y-%m-%d %H:%i:%s") DESC
    #"""
    try:
        cursor = conn.cursor()
        # read search keywords from table topicinfo
        cursor.execute(select_topic)  # filter by date: >_< , include >, exclude <
        for res in cursor.fetchall():
            yield res[0]
        # print "$"*20, "There are %d topics to process" % len(todo_topic_list)
    except Exception as e:
        traceback.print_exc()
        print dt.now().strftime("%Y-%m-%d %H:%M:%S"), "Unable read topic from database.."
