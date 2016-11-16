#coding=utf-8
import sys, time, os, signal, traceback
from datetime import datetime as dt, timedelta
import MySQLdb as mdb
import multiprocessing as mp
from baidu_spider import parse_baidu_search_page
from database_operator import (
    connect_database,
    write_baidu_topic_into_db,
    read_topics_from_db
)
reload(sys)
sys.setdefaultencoding('utf-8')

DATE_ERANGES = ['day', 'week', 'month']

def topic_info_generator(topic_jobs, topic_results):
    """
    Producer for urls and topics, Consummer for topics
    """
    cp = mp.current_process()
    while True:
        print dt.now().strftime("%Y-%m-%d %H:%M:%S"), "Generate Urls Process pid is %d" % (cp.pid)
        kw = topic_jobs.get()
        for dr in DATE_ERANGES:  # do 3 times search, look whether solve the problem: day>week>month
            baidu_result = parse_baidu_search_page(kw, dr)
            topic_results.put(baidu_result['data'])
        topic_jobs.task_done()
    

def topic_db_writer(topic_results):
    """
    Consummer for topics
    """
    cp = mp.current_process()
    while True:
        print dt.now().strftime("%Y-%m-%d %H:%M:%S"), "Write Topics Process pid is %d" % (cp.pid)
        with connect_database() as cursor:
            topic_record = topic_results.get()
            write_status = write_baidu_topic_into_db(cursor, topic_record)
            # print topic_record.items()
            topic_results.task_done()
    

def create_processes(func, args, concurrency):
    for _ in range(concurrency):
        sub_proc = mp.Process(target=func, args=args)
        sub_proc.daemon = True
        sub_proc.start()


def add_topic_jobs(target):
    todo = 0
    one_week_ago = (dt.today() - timedelta(6)).strftime("%Y-%m-%d")
    try:
        conn = connect_database()
        if not conn:
            return False
        list_of_kw = read_topics_from_db(conn.cursor(), one_week_ago)
        for kw in list_of_kw:
            todo += 1
            target.put(kw)
    except mdb.OperationalError as e:
            traceback.print_exc()
            print dt.now().strftime("%Y-%m-%d %H:%M:%S")
    except Exception as e:
        traceback.print_exc()
    finally:
        conn.close()
        return todo


def run_all_worker():
    try:
        # Producer is on !!!
        topic_jobs = mp.JoinableQueue()
        topic_results = mp.JoinableQueue()
        create_processes(topic_info_generator, (topic_jobs, topic_results), 1)
        create_processes(topic_db_writer, (topic_results,), 2)

        cp = mp.current_process()
        print dt.now().strftime("%Y-%m-%d %H:%M:%S"), "Run All Works Process pid is %d" % (cp.pid)
        num_of_topics = add_topic_jobs(target=topic_jobs)
        print "<"*10, "There are %d topics to process" % num_of_topics, ">"*10
        topic_jobs.join()
        topic_results.join()

        print "+"*10, "topic_jobs' length is ", topic_jobs.qsize()
        print "+"*10, "topic_results' length is ", topic_results.qsize()
    except Exception as e:
        traceback.print_exc()
        print dt.now().strftime("%Y-%m-%d %H:%M:%S"), "Exception raise in Rn all Work"
    except KeyboardInterrupt:
        print dt.now().strftime("%Y-%m-%d %H:%M:%S"), "Interrupted by you and quit in force, but save the results"

def test_parse_baidu_results():    
    try:
        conn = connect_database()
        if not conn:
            return False
        list_of_kw = read_topics_from_db(conn.cursor())
        for kw in list_of_kw:
            for dr in DATE_ERANGES:  # do 3 times search, look whether solve the problem: day>week>month
                baidu_result = parse_baidu_search_page(kw, dr)
                print baidu_result['data']['search_url'], baidu_result['data']['hit_num']
    except Exception as e:
        traceback.print_exc()
    finally:
        conn.close()


if __name__=="__main__":
    print "\n" + "Began Scraping Baidu time is : %s" % dt.now().strftime("%Y-%m-%d %H:%M:%S") + "\n"
    start = time.time()
    run_all_worker()
    # test_parse_baidu_results()
    print "*"*10, "Total Scrping Baidu Time Consumed : %d seconds" % (time.time() - start), "*"*10
