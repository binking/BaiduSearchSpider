#coding=utf-8
import sys, time, re, os
import requests, urllib, traceback
import json, traceback, bs4
from datetime import datetime as dt, timedelta
from urlparse import urlparse, parse_qs
from bs4 import BeautifulSoup as bs
from requests.packages.urllib3.exceptions import InsecureRequestWarning
from config import *
from utils import *
from requests.exceptions import (
    ProxyError,
    Timeout,
    ConnectionError,
    ConnectTimeout,
)

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
reload(sys)
sys.setdefaultencoding('utf-8')

# %3D:=; %2C:,; %7C:|;
# --connect-timeout
# ReadTimeout: HTTPSConnectionPool(host='www.baidu.com', port=443): Read timed out. (read timeout=10)

def parse_baidu_search_page_v2(keyword, date_range, proxy={},num_tries=3, wait_time=10):
    print "Baidu searching for ", keyword, "in 1 ", date_range
    err_no = FAILED; err_msg = ERROR_MSG_DICT[FAILED]
    # url = QUERY_URL_DICT[date_range].format(kw=urllib.quote(keyword))
    start_timestamp, end_timestamp = gen_gpc(date_range)
    # import ipdb; ipdb.set_trace()
    try:
        kw = urllib.quote(keyword.encode('utf8'))
    except (UnicodeEncodeError, KeyError) as e:
        print "Quoting Chinese keyword %s to url string FAILED" % keyword
        kw = keyword

    beat_it = BAIDU_CURL_STR.format(kw=kw, start=start_timestamp, end=end_timestamp)
    url, post_data = curl_str2post_data(beat_it)
    if not(url and post_data):
        return err_no
    data = { "createdate": dt.now().strftime("%Y-%m-%d %H:%M:%S"), 
              "uri": url, "search_url": url, "search_keyword": keyword,
              "date_range": date_range, "hit_num": 0}
    for attempt in range(1, num_tries+1):
        try:
            r = requests.get(url, verify=False, params=data, headers=HEADERS, proxies=proxy, timeout=wait_time)
            baidu_parser = bs(r.text, "html.parser")
            none_res_div = baidu_parser.find("div", {"class": "content_none"})
            if none_res_div:  # no search result
                # data["hit_num"] = 0
                err_no = SUCCESSED
                err_msg = ERROR_MSG_DICT[SUCCESSED]
                break
            a_tag = baidu_parser.find("a", {
                "data-click": re.compile(r"{*"), 
                "href": re.compile(r"http://www.baidu.com/link\?url=*")
            })
            if a_tag:
                num_tag = baidu_parser.find("div", {"class": "nums"})
                if num_tag:
                    num_str = re.search(re.compile(r'((\d*),?)*(\d+)'), num_tag.text)  # extract number
                    data["hit_num"] = str_2_int(num_str.group(0)) if num_str else -1
                    err_no = SUCCESSED
                    err_msg = ERROR_MSG_DICT[SUCCESSED]
            else:  # wrong result
                with open('./html/Baidu_%s_%s.html' % (keyword.encode('utf8'), dt.now()), 'w') as fw:
                    # null_parser = BS(open('2016-11-15 16:11:06.167554_朴施妍1114生日快乐.html', 'r').read(), "html.parser")
                    # prevent UnicodeEncodeError: 'ascii' codec can't encode characters in position: ordinal not in range(128)
                    print >>fw, r.text.encode("utf8")  # save the unknow html
            break # success and jump out of loop
        except Timeout as e:
            print dt.now().strftime("%Y-%m-%d %H:%M:%S"), ERROR_MSG_DICT[NETWORK_TIMEOUT],
            handle_sleep(5*attempt)
        except ConnectionError as e:
            traceback.print_exc()
            print dt.now().strftime("%Y-%m-%d %H:%M:%S"), ERROR_MSG_DICT[NETWORK_CONNECTION_ERROR],
            handle_sleep(5*attempt)
        except ProxyError as e:
            # traceback.print_exc()
            print dt.now().strftime("%Y-%m-%d %H:%M:%S"), ERROR_MSG_DICT[NETWORK_PROXY_ERROR],
            handle_proxy_error(5*attempt)
        except Exception as e:
            traceback.print_exc()
            print dt.now().strftime("%Y-%m-%d %H:%M:%S"), "Parsed topic %s Failed..." % keyword
            handle_sleep(5*attempt)
        print data
    return {"err_no": err_no, "err_msg": err_msg, "data": data}


def test_parse_baidu_results():
    list_of_kw = ["海贼王", "后街男孩", "百度", "百度音乐", "MySQL deadlock caused by concurrent INSERT and SELECT"]
    # list_of_kw = ["MySQL deadlock caused by concurrent INSERT and SELECTgdfghfhgfh"]  # no results
    for kw in list_of_kw:
        for date_range in ['day', 'week', 'month']:
            print parse_baidu_search_page_v2(kw, date_range)
            # print parse_baidu_search_page_v2(kw, date_range)
