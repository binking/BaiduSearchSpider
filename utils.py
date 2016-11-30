#coding=utf-8
import time
import traceback
import multiprocessing as mp
from datetime import datetime as dt, timedelta
from abuyun_proxy import change_tunnel

def wrap_print(tag, center, repeat=10):
    print tag * repeat,
    print center,
    print tag * repeat


def handle_sleep(seconds):
    print "Sleeping %d seconds " % seconds, 'zZ'*10
    time.sleep(seconds)

def handle_proxy_error(seconds):
    print "Sleep %d seconds " % seconds, 
    handle_sleep(seconds)
    changed_proxy = change_tunnel()  # Change IP tunnel of Abuyun
    if changed_proxy:
        print "and change IP to %s " % changed_proxy.get("ip_addr")
    else:
        print "but Change Proxy Error"

def str_2_int(num_str):
    try:
        return int(num_str.replace(',', ''))
    except:
        return -1

def gen_gpc(date_range):
    """
    Given date range, generate the from-to timestamp of baidu search url
    param date_range(str): 'day', 'week', 'month'
    return (start_timestamp, end_timestamp): (str, str)
    """
    DATE_RANGE_DAYS = {'day': 1, 'week':2, 'month': 30}
    today = dt.now()
    ago = dt.today() - timedelta(DATE_RANGE_DAYS[date_range])
    end_timestamp = int((today - dt(1970, 1, 1)).total_seconds())
    start_timestamp = int((ago - dt(1970, 1, 1)).total_seconds())
    return str(start_timestamp), str(end_timestamp)

def curl_str2post_data(baidu_curl):
    """
    Given curl that was cpoied from Chrome, no matter baidu or sogou, 
    parse it and then get url and the data you will post/get with requests
    """
    url = ""
    post_data = {}
    # Format api access
    tokens = baidu_curl.split("'")
    try:
        for i in range(0, len(tokens)-1, 2):
            if tokens[i].startswith("curl"):
                url = tokens[i+1]
            elif "-H" in tokens[i]:
                attr, value = tokens[i+1].split(": ")  # be careful space
                post_data[attr] = value
    except Exception as e:
        print "!"*20, "Parsed cURL Failed"
        traceback.print_exc()
    return url, post_data


def create_processes(func, args, concurrency):
    for _ in range(concurrency):
        sub_proc = mp.Process(target=func, args=args)
        sub_proc.daemon = True
        sub_proc.start()