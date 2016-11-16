import traceback
from datetime import datetime as dt
from config import *
from utils import handle_sleep
from abuyun_proxy import change_tunnel


def catch_database_error(db_func):
	"""
	A decrator that catch exceptions and print relative infomation
	"""
	def handle_exception(*args, **kargs):
		try:
			return db_func(*args, **kargs)
		except(mdb.ProgrammingError, mdb.OperationalError) as e:
	        traceback.print_exc()
	        if 'MySQL server has gone away' in e.message:
	            print dt.now().strftime("%Y-%m-%d %H:%M:%S"), ERROR_MSG_DICT[DB_SEVER_GONE_AWAY],
	        elif 'Deadlock found when trying to get lock' in e.message:
	            print dt.now().strftime("%Y-%m-%d %H:%M:%S"), ERROR_MSG_DICT[DB_FOUND_DEADLOCK],
	        elif 'Lost connection to MySQL server' in e.message:
	            print dt.now().strftime("%Y-%m-%d %H:%M:%S"), ERROR_MSG_DICT[DB_LOST_CONNECTION],
	        elif 'Lock wait timeout exceeded' in e.message:
	        	print dt.now().strftime("%Y-%m-%d %H:%M:%S"), ERROR_MSG_DICT[DB_LOCK_WAIT_TIMEOUT],
	        elif e.args[0] in [1064, 1366]:
	            print dt.now().strftime("%Y-%m-%d %H:%M:%S"), ERROR_MSG_DICT[DB_UNICODE_ERROR],
	        else:
	            print dt.now().strftime("%Y-%m-%d %H:%M:%S"), ERROR_MSG_DICT[DB_UNKNOW_ERROR],
	    except Exception as e:
	        traceback.print_exc()
	        print dt.now().strftime("%Y-%m-%d %H:%M:%S"), ERROR_MSG_DICT[DB_WRITE_FAILED],
	return handle_exception


def catch_network_error(req_func):
	def handle_exception(*args. **kargs):
		try:
			return req_func(*args, **kargs)
		except Timeout as e:
            print dt.now().strftime("%Y-%m-%d %H:%M:%S"), ERROR_MSG_DICT[NETWORK_TIMEOUT],
            handle_sleep(5*attempt)
        except ConnectionError as e:
            # traceback.print_exc()
            print dt.now().strftime("%Y-%m-%d %H:%M:%S"), ERROR_MSG_DICT[NETWORK_CONNECTION_ERROR],
            handle_sleep(5*attempt)
        except ProxyError as e:
            # traceback.print_exc()
            print dt.now().strftime("%Y-%m-%d %H:%M:%S"), ERROR_MSG_DICT[NETWORK_PROXY_ERROR],
            handle_proxy_error(5*attempt)
        except Exception as e:
            traceback.print_exc()
            # change_tunnel()
            print dt.now().strftime("%Y-%m-%d %H:%M:%S"), ERROR_MSG_DICT[REQUEST_ERROR]
            handle_sleep(5*attempt)
    return handle_exception()