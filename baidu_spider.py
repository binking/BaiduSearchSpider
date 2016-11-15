#coding=utf-8
import sys, time, re, os
import requests, urllib, traceback
import json, traceback, bs4
from datetime import datetime as dt, timedelta
from urlparse import urlparse, parse_qs
from bs4 import BeautifulSoup as bs
from abuyun_proxy import change_tunnel
from requests.exceptions import (
    ProxyError,
    Timeout,
    ConnectionError,
    ConnectTimeout,
)

reload(sys)
sys.setdefaultencoding('utf-8')

SUCCESSED = 1
FAILED = -1
PARSE_ERROR = -2
ACCESS_URL_ERROR = -3
WRITE_DB_ERROR = -4
SYNTAX_ERROR = -5
IGNORE_RECORD = -6

ABUYUN_USER = "H778U07K14M4250P"
ABUYUN_PASSWD = "FE04DDEF88A0CC9B"
ABUYUN_HOST = "proxy.abuyun.com"
ABUYUN_PORT = "9010"


BAIDU_CURL_STR = """curl 'https://www.baidu.com/s?ie=utf-8&mod=1&isbd=1&isid=73D1C879AE736221&ie=utf-8&f=8&rsv_bp=1&tn=baidu&wd={kw}&rsv_pq=cbbdd19c00019da4&rsv_t=f3caeinSf3EIdOwk9OYq11erzbHt0TEYxnpR%2BF2TtmSJA%2FT875hqGQSVIWk&rqlang=cn&rsv_enter=1&gpc=stf={start},{end}|stftype=1&tfflag=1&rsv_sid=undefined&_ss=1&clist=&hsug=&f4s=1&csor=3&_cr1=29723' -H 'Cookie: BAIDUID=73D1C894D6EA1C8C35BAA51C16979AE7:FG=1; BIDUPSID=73D1C894D6EA1C8C35BAA51C16979AE7; PSTM=1478583844; __cfduid=dd3986b6e9eb2e4dd47d954b8a76ae6801478744553; BDUSS=GVkYkdnSWNzeEdUb2FyMHNZNTRRR3hsZFVCblhhLXA0YWplYi1CNkZpM0RmVXRZSVFBQUFBJCQAAAAAAAAAAAEAAABPwzQQOTY1MDc2Mzc3AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAMPwI1jD8CNYV; BD_CK_SAM=1; PSINO=2; H_PS_PSSID=1445_21078_21454_21409_21377_21526_21190_20930; BD_UPN=12314753; sugstore=1; H_PS_645EC=f3caeinSf3EIdOwk9OYq11erzbHt0TEYxnpR%2BF2TtmSJA%2FT875hqGQSVIWk' -H 'is_xhr: 1' -H 'Accept-Encoding: gzip, deflate, sdch, br' -H 'Accept-Language: zh-CN,zh;q=0.8' -H 'User-Agent: Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.99 Safari/537.36' -H 'Accept: */*' -H 'Referer: https://www.baidu.com/s?ie=utf-8&f=8&rsv_bp=1&tn=baidu&wd={kw}&rsv_pq=cbbdd19c00019da4&rsv_t=f3caeinSf3EIdOwk9OYq11erzbHt0TEYxnpR%2BF2TtmSJA%2FT875hqGQSVIWk&rqlang=cn&rsv_enter=1&gpc=stf={start},{end}|stftype=1&tfflag=1' -H 'X-Requested-With: XMLHttpRequest' -H 'Connection: keep-alive' -H 'is_referer: https://www.baidu.com/s?ie=utf-8&f=8&rsv_bp=1&tn=baidu&wd={kw}&rsv_pq=cbbdd19c00019da4&rsv_t=bf0dy07frZ1UKtJW01IYbfJGeI%2BhOMsDi%2BC%2BEQBWBb8oxFu2c53n31gktrg&rqlang=cn' --compressed"""
# %3D:=; %2C:,; %7C:|;

def handle_sleep(seconds):
    print "Sleeping %d seconds " % seconds, 'zZ'*10
    time.sleep(seconds)

def str_2_int(num_str):
    try:
        return int(num_str.replace(',', ''))
    except:
        return -1

def gen_gpc(date_range):
    DATE_RANGE_DAYS = {'day': 1, 'week':2, 'month': 30}
    if date_range not in DATE_RANGE_DAYS.keys():
        print "Wrong date range"

    today = dt.now()
    ago = dt.today() - timedelta(DATE_RANGE_DAYS[date_range])
    end_timestamp = int((today - dt(1970, 1, 1)).total_seconds())
    start_timestamp = int((ago - dt(1970, 1, 1)).total_seconds())

    return str(start_timestamp), str(end_timestamp)

def curl_str2post_data(baidu_curl):
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


def parse_baidu_search_page(keyword, date_range, num_tries=3, wait_time=10):
    """
    Given keyword, form the Sougou search url and parse the search results page
    Formatted by Chrome: https://www.baidu.com/s?ie=UTF-8&wd=%E5%90%8E%E8%A1%97%E7%94%B7%E5%AD%A9
    param keywords:list of keywords
    return : {err_no: , err_msg: , data: 
        { uri: , createdate:, search_url:, }}
    """
    print "Sougou searching for ", keyword, "in 1 ", date_range
    err_no = SUCCESSED; err_msg = "Successed"
    # url = QUERY_URL_DICT[date_range].format(kw=urllib.quote(keyword))
    start_timestamp, end_timestamp = gen_gpc(date_range)
    beat_it = BAIDU_CURL_STR.format(
        kw=urllib.quote(keyword), 
        start=start_timestamp, 
        end=end_timestamp,
        # host=ABUYUN_HOST,
        # port=ABUYUN_PORT,
        # user=ABUYUN_USER,
        # passwd=ABUYUN_PASSWD
    )
    url, post_data = curl_str2post_data(beat_it)
    data = { "createdate": dt.now().strftime("%Y-%m-%d %H:%M:%S"), 
              "uri": url, "search_url": url, 
              "search_keyword": keyword,
              "date_range": date_range,
              "hit_num": -1,
              "top_url": '',
              "top_title": '',
               }
    for attempt in range(1, num_tries+1):
        try:
            baidu_parser = bs(os.popen(beat_it), "html.parser")
            none_res_div = baidu_parser.find("div", {"class": r"content_none"})
            if none_res_div:
                data["hit_num"] = 0
                break
            a_tag = baidu_parser.find("a", {
                "data-click": re.compile(r"{*"), 
                "href": re.compile(r"http://www.baidu.com/link\?url=*")
            })
            if a_tag:
                baidu_jump_link = a_tag.get("href", "")
                if not baidu_jump_link:
                    print "No Baidu Jump Link"
                    return {}
                if baidu_jump_link:
                    third_url = requests.get(baidu_jump_link).url
                    data["top_url"] = third_url if third_url else baidu_jump_link
                    data["top_title"] = a_tag.text
                num_tag = baidu_parser.find("div", {"class": "nums"})
                if num_tag:
                    num_str = re.search(re.compile(r'((\d*),?)*(\d+)'), num_tag.text)
                    data["hit_num"] = str_2_int(num_str.group(0)) if num_str else -1
            else:  # no result
                with open('Baidu_%s_%s.html' % (keyword, dt.now()), 'w') as fw:
                    # null_parser = BS(open('2016-11-15 16:11:06.167554_朴施妍1114生日快乐.html', 'r').read(), "html.parser")
                    print >>fw, baidu_parser  # save the unknow html
                print "No Baidu Jump Link"
                return {}
            break # success and jump out of loop
        except Exception as e:
            traceback.print_exc()
            err_no = FAILED
            err_msg = e.message
            change_tunnel()
            print dt.now().strftime("%Y-%m-%d %H:%M:%S"), "Parsed topic %s Failed..." % keyword
            handle_sleep(5*attempt)
    return {"err_no": err_no, "err_msg": err_msg, "data": data}


def test_parse_baidu_results():
    # list_of_kw = ["海贼王", "后街男孩", "百度", "百度音乐", "MySQL deadlock caused by concurrent INSERT and SELECT"]
    list_of_kw = ["MySQL deadlock caused by concurrent INSERT and SELECTgdfghfhgfh"]  # no results
    for kw in list_of_kw:
        for date_range in ['week', 'day', 'month']:
            print parse_baidu_search_page(kw, date_range)


test_parse_baidu_results()