#coding=utf-8

# Related to  aybu cloud proxy
ABUYUN_USER = "H778U07K14M4250P"
ABUYUN_PASSWD = "FE04DDEF88A0CC9B"
ABUYUN_HOST = "proxy.abuyun.com"
ABUYUN_PORT = "9010"

# Related to send requests
HEADERS = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_3) AppleWebKit/600.5.17 (KHTML, like Gecko) Version/8.0.5 Safari/600.5.17'}

# Execute result number
SUCCESSED = 1
FAILED = -1
REQUEST_ERROR = -100
PARSE_HTML_ERROR = -101
ACCESS_URL_ERROR = -102
IGNORE_RECORD = -102

# Database Error number using 2XX
DB_WRITE_FAILED = -200
DB_CANNOT_CONNECT = -201
DB_LOST_CONNECTION = -203
DB_SYNTAX_ERROR = -204
DB_LOCK_WAIT_TIMEOUT = -205
DB_FOUND_DEADLOCK = -206
DB_SEVER_GONE_AWAY = -213
DB_UNICODE_ERROR = -207
DB_UNKNOW_ERROR = -299

# Network Error number using 3XX
NETWORK_CONNECTION_ERROR = -300
NETWORK_PROXY_ERROR = -301
NETWORK_TIMEOUT = -302

# Execute result message
ERROR_MSG_DICT = {
    SUCCESSED: "Succeeded",
    FAILED: "Failed",
    REQUEST_ERROR: "Request Target Fialed",
    PARSE_HTML_ERROR: "Parsed url or html FAILED",
    ACCESS_URL_ERROR: "Send requests FAILED",
    IGNORE_RECORD: "The record would be ignored",

    # for database
    DB_WRITE_FAILED: "Write data into database FAILED",
    DB_CANNOT_CONNECT: "Can't connect to MySQL.(服务器资源紧张，导致无法连接)",
    DB_LOCK_WAIT_TIMEOUT: "Lock wait timeout exceeded, try restarting transaction and check if someone manages the table",
    DB_SEVER_GONE_AWAY: "MySQL server has gone away",
    DB_SYNTAX_ERROR: "You have an error in your SQL syntax.(SQL语句有语法错误)",
    DB_LOST_CONNECTION: "Lost connection to MySQL server during query",
    DB_FOUND_DEADLOCK: "Deadlock found when trying to get lock; try restarting transaction",
    DB_UNICODE_ERROR: "Incorrect string value.(不能写入字符串到数据库)",
    DB_UNKNOW_ERROR: "Unknown Program or Operation Errors",

    # for network
    NETWORK_CONNECTION_ERROR: "Connection Error",
    NETWORK_TIMEOUT: "Timeout",
    NETWORK_PROXY_ERROR: "(连接池满了)Max retries exceeded with url",
}

BAIDU_CURL_STR = """curl 'https://www.baidu.com/s?ie=utf-8&mod=1&isbd=1&isid=73D1C879AE736221&ie=utf-8&f=8&rsv_bp=1&tn=baidu&wd={kw}&rsv_pq=cbbdd19c00019da4&rsv_t=f3caeinSf3EIdOwk9OYq11erzbHt0TEYxnpR%2BF2TtmSJA%2FT875hqGQSVIWk&rqlang=cn&rsv_enter=1&gpc=stf={start},{end}|stftype=1&tfflag=1&rsv_sid=undefined&_ss=1&clist=&hsug=&f4s=1&csor=3&_cr1=29723' -H 'Cookie: BAIDUID=73D1C894D6EA1C8C35BAA51C16979AE7:FG=1; BIDUPSID=73D1C894D6EA1C8C35BAA51C16979AE7; PSTM=1478583844; __cfduid=dd3986b6e9eb2e4dd47d954b8a76ae6801478744553; BDUSS=GVkYkdnSWNzeEdUb2FyMHNZNTRRR3hsZFVCblhhLXA0YWplYi1CNkZpM0RmVXRZSVFBQUFBJCQAAAAAAAAAAAEAAABPwzQQOTY1MDc2Mzc3AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAMPwI1jD8CNYV; BD_CK_SAM=1; PSINO=2; H_PS_PSSID=1445_21078_21454_21409_21377_21526_21190_20930; BD_UPN=12314753; sugstore=1; H_PS_645EC=f3caeinSf3EIdOwk9OYq11erzbHt0TEYxnpR%2BF2TtmSJA%2FT875hqGQSVIWk' -H 'is_xhr: 1' -H 'Accept-Encoding: gzip, deflate, sdch, br' -H 'Accept-Language: zh-CN,zh;q=0.8' -H 'User-Agent: Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.99 Safari/537.36' -H 'Accept: */*' -H 'Referer: https://www.baidu.com/s?ie=utf-8&f=8&rsv_bp=1&tn=baidu&wd={kw}&rsv_pq=cbbdd19c00019da4&rsv_t=f3caeinSf3EIdOwk9OYq11erzbHt0TEYxnpR%2BF2TtmSJA%2FT875hqGQSVIWk&rqlang=cn&rsv_enter=1&gpc=stf={start},{end}|stftype=1&tfflag=1' -H 'X-Requested-With: XMLHttpRequest' -H 'Connection: keep-alive' -H 'is_referer: https://www.baidu.com/s?ie=utf-8&f=8&rsv_bp=1&tn=baidu&wd={kw}&rsv_pq=cbbdd19c00019da4&rsv_t=bf0dy07frZ1UKtJW01IYbfJGeI%2BhOMsDi%2BC%2BEQBWBb8oxFu2c53n31gktrg&rqlang=cn' --compressed"""
