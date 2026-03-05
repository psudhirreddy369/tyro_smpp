import os
from datetime import datetime
from os import path, makedirs
import json
import logging, sys, glob, os, re, datetime
import configparser
import threading
import pytz
write_lock = threading.Lock()  # Thread-safe file writing

#config = configparser.ConfigParser()
#config.read('mobismart.ini')

#INSERT_QUERIES_PATH=config['DEFAULT']['INSERT_QUERIES_PATH']

def insert_report(rcvsmsmysql,INSERT_QUERIES_PATH):
    try:
        rec_inst="insertsms_"
        # india_timezone = pytz.timezone("Asia/Kolkata")
        # current_time_in_india = datetime.now(india_timezone)
        # timestamp = current_time_in_india.strftime('%Y-%m-%d_%H-%M')
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M")
        #sql_file_path = f"{mysql_query_file_path}{base_file_name}_{timestamp}.sql"
        sql_file_path = f"{INSERT_QUERIES_PATH}{rec_inst}{timestamp}.sql"
        if os.path.exists(f"{sql_file_path}"):
            mode = 'a'
        else:
            mode = 'w'
        with write_lock, open(sql_file_path, mode) as sql_file:
            #rcvsmsmysql.update_data(rcvsmsquery)
            sql_file.write(rcvsmsmysql)
    except Exception as e:
            logging.error('Exception :%s',str(e))
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(exc_type, fname, exc_tb.tb_lineno)
            return {"message": str(e)}
    
def submitted_report(rcvsmsmysql,INSERT_QUERIES_PATH):
    try:
        rec_submit="submitsms_"
        # india_timezone = pytz.timezone("Asia/Kolkata")
        # current_time_in_india = datetime.now(india_timezone)
        # timestamp = current_time_in_india.strftime('%Y-%m-%d_%H-%M')
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H")
        #sql_file_path = f"{mysql_query_file_path}{base_file_name}_{timestamp}.sql"
        sql_file_path = f"{INSERT_QUERIES_PATH}{rec_submit}{timestamp}.log"
        print("&&&&&&&&&&&&&")
        print(sql_file_path)
        if os.path.exists(f"{sql_file_path}"):
            mode = 'a'
        else:
            mode = 'w'
        print(mode)
        with write_lock,open(sql_file_path, mode) as sql_file:
            #rcvsmsmysql.update_data(rcvsmsquery)
            sql_file.write(rcvsmsmysql)
    except Exception as e:
            logging.error('Exception :%s',str(e))
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(exc_type, fname, exc_tb.tb_lineno)
            return {"message": str(e)}

def delivered_report(rcvsmsmysql,INSERT_QUERIES_PATH):
    try:
        rec_deliver="deliverysms_"
        # india_timezone = pytz.timezone("Asia/Kolkata")
        # current_time_in_india = datetime.now(india_timezone)
        # timestamp = current_time_in_india.strftime('%Y-%m-%d_%H-%M')
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H")
        #sql_file_path = f"{mysql_query_file_path}{base_file_name}_{timestamp}.sql"
        sql_file_path = f"{INSERT_QUERIES_PATH}{rec_deliver}{timestamp}.log"
        print(sql_file_path)
        if os.path.exists(f"{sql_file_path}"):
            mode = 'a'
        else:
            mode = 'w'
        with write_lock, open(sql_file_path, mode) as sql_file:
            #rcvsmsmysql.update_data(rcvsmsquery)
            sql_file.write(rcvsmsmysql)
    except Exception as e:
            print("*************************")
            print('Exception :%s',str(e))
            logging.error('Exception :%s',str(e))
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(exc_type, fname, exc_tb.tb_lineno)
            return {"message": str(e)}        