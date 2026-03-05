# Import KafkaConsumer from Kafka library
from os import path,makedirs
import sys
import json
from datetime import date,datetime
from sqlbulkfile import insert_report,submitted_report,delivered_report
import logging, sys, glob, os

import threading

import logging

import configparser
import base64
#from mysqldb import MySQLDB
from random import randint, randrange

from kafka import KafkaConsumer

from dotenv import load_dotenv
import os

# Load environment variables from .env
load_dotenv()

# Environment variables

KAFKA_BOOTSTRAP_SERVER=os.getenv('KAFKA_BOOTSTRAP_SERVER')
RECEIVE_SMS_TOPIC=os.getenv('RECEIVE_SMS_TOPIC')
SUBMIT_SMS_TOPIC=os.getenv('SUBMIT_SMS_TOPIC')
DELIVER_SMS_TOPIC=os.getenv('DELIVER_SMS_TOPIC')
INSERT_MANY_RECORD_COUNT=int(os.getenv('INSERT_MANY_RECORD_COUNT'))
INSERT_QUERIES_PATH=os.getenv('INSERT_QUERIES_PATH')

# logdirectory='./logs/'+date.today().strftime("%Y-%m-%d")+'/'
logdirectory='./logs/'

if not path.exists(logdirectory):
    makedirs(logdirectory)
    print("Directory " , logdirectory ,  " Created ")
else:
    print("Directory " , logdirectory ,  " already exists")

logging.basicConfig(filename=logdirectory+'/KafkaTopic2MySQL.log', filemode='a',level=logging.INFO, format='%(asctime)s - %(levelname)s - %(funcName)s - %(message)s')

processed_msg_ids = set()  # Set to keep track of processed message IDs

def receivedMessage2MySQL():

    # rcvsmsmysql=MySQLDB()

    # Initialize consumer variable
    rcvsmsconsumer = KafkaConsumer (RECEIVE_SMS_TOPIC, bootstrap_servers =[KAFKA_BOOTSTRAP_SERVER])

    # rcvsms_db_values = None

    # Read and print message from consumer
    for msg in rcvsmsconsumer:

        try:
            print("MESSAGE FROM CONSUMER:" , msg)
            #  Encode ASCII to Bytes encode('iso-8859-15')
            #  Decode Bytes into UTF-8 Format decode("utf-8")

            rcvsmsmessage=json.loads(msg.value.decode("utf-8"))
            logging.info("Topic Name=%s,Message=%s"%(msg.topic,rcvsmsmessage))

            hexstr=rcvsmsmessage['message'] #.encode("utf-8").hex()
            
            msg_id = rcvsmsmessage['messageid']
            # Check for duplicate message
            if msg_id in processed_msg_ids:
                logging.info(f"Duplicate message detected: {msg_id}")
                continue
            else:
                processed_msg_ids.add(msg_id)

            ####### FOR BULK INSERT INTO SQL LOG FILE ################################
            hexstr_str=""
            if rcvsmsmessage['messagetype']=='T':
                hexstr1=base64.b64encode(rcvsmsmessage['message'].encode('utf-8',errors="ignore"))
                hexstr_str = hexstr1.decode('utf-8',errors="ignore")
            else:
                hexstr1=base64.b64encode(rcvsmsmessage['message'].encode('utf-16be',errors="ignore"))
                hexstr_str = hexstr1.decode('utf-16be',errors="ignore")   
            rcvsmsinst=f"insert ignore into sms_inserted_report_V2 (msgId,inTime,sender,destination,message,msgType,systemId,peid,templateid,status,registerdelivery,seq_no)VALUES('{msg_id}','{rcvsmsmessage['inTime']}','{rcvsmsmessage['sender']}','{rcvsmsmessage['dest']}','{hexstr_str}','{rcvsmsmessage['messagetype']}','{rcvsmsmessage['user']}','{rcvsmsmessage['peid']}','{rcvsmsmessage['templateid']}','{rcvsmsmessage['status']}','{rcvsmsmessage['deliveryreport']}','{randint(100, 9999)}');\n"
            print(rcvsmsinst)
            insert_report(rcvsmsinst,INSERT_QUERIES_PATH)
            ##############################################

            # hexstr=rcvsmsmessage['message'] #.encode("utf-8").hex()
            
            #if rcvsmsmessage['messagetype']=='T':
            #     hexstr=rcvsmsmessage['message'].encode("utf-8").hex()

            #  Single Record Insert

            # rcvsmsquery="insert into nxl2.inserted_report_V2 (msgId,inTime,sender,destination,message,msgType,systemId,peid,templateid,status,registerdelivery ) \
            #         VALUES('"+rcvsmsmessage['messageid']+"','"+rcvsmsmessage['inTime']+"','"+rcvsmsmessage['sender']+"','"+rcvsmsmessage['dest']+"','"+hexstr+"','"+ \
            #                 rcvsmsmessage['messagetype']+"','"+rcvsmsmessage['user']+"','"+rcvsmsmessage['peid']+"','"+rcvsmsmessage['templateid']+"','"+rcvsmsmessage['status']+"','"+rcvsmsmessage['deliveryreport']+"')"

            # rcvsmsmysql.update_data(rcvsmsquery)

            #  Batch Insert
            # rcvsmsquery="insert into sms_inserted_report_V2 (msgId,inTime,sender,destination,message,msgType,systemId,peid,templateid,status,registerdelivery,seq_no ) \
            #         VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

            # rcvsmsrow=(rcvsmsmessage['messageid'],rcvsmsmessage['inTime'],rcvsmsmessage['sender'],rcvsmsmessage['dest'],hexstr , \
            #             rcvsmsmessage['messagetype'],rcvsmsmessage['user'],rcvsmsmessage['peid'],rcvsmsmessage['templateid'],rcvsmsmessage['status'],rcvsmsmessage['deliveryreport'],randint(100, 9999))

            # if rcvsms_db_values is None:
            #     rcvsms_db_values=[rcvsmsrow]
            # else:
            #     rcvsms_db_values.append(rcvsmsrow)

            # if len(rcvsms_db_values)==INSERT_MANY_RECORD_COUNT:
            #     logging.info('Query:%s, Values:%s',rcvsmsquery,rcvsms_db_values)
            #     # logging.info(rcvsms_db_values)
                
            #     rcvsmsmysql.execute_many(rcvsmsquery,rcvsms_db_values)
            #     rcvsms_db_values=None

        except Exception as e:
            logging.error('Exception :%s',str(e))
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(exc_type, fname, exc_tb.tb_lineno)
            return {"message": str(e)}

def submittedMessage2MySQL():

    # subsmsmysql=MySQLDB()

    # Initialize consumer variable
    subsmsconsumer = KafkaConsumer (SUBMIT_SMS_TOPIC, bootstrap_servers =[KAFKA_BOOTSTRAP_SERVER])

    # subsms_db_values = None

    # Read and print message from consumer
    for msg in subsmsconsumer:

        try:

            subsmsdict=json.loads(msg.value.decode("utf-8"))
            logging.info("Topic Name=%s,Message=%s"%(msg.topic,subsmsdict))

            logging.info(subsmsdict)
            # msg_id = subsmsdict['msgId']
            # # Check for duplicate message
            # if msg_id in processed_msg_ids:
            #     logging.info(f"Duplicate message detected: {msg_id}")
            #     continue
            # else:
            #     processed_msg_ids.add(msg_id)

            ####### FOR BULK INSERT INTO SQL LOG FILE ################################
            rcvsmsinst=f"insert ignore into sms_submitted_report_V2 (msgId,subTime,smscId,smsc,status,errcode)VALUES('{subsmsdict['msgId']}','{subsmsdict['subTime']}','{subsmsdict['smscId']}','{subsmsdict['smsc']}','SUBMITTED','0');\n"
            submitted_report(rcvsmsinst,INSERT_QUERIES_PATH)
            ##############################################################################

            # if subsmsdict['msgcount']>1:

            #     if subsmsdict['msg_part_num']==1:

            #         subsmsquery="insert into sms_submitted_report_V2 (msgId,subTime,smscId,smsc,status,errcode) "+\
            #         " VALUES('"+subsmsdict['msgId']+"','"+subsmsdict['subTime']+"','"+subsmsdict['smscId']+"','"+subsmsdict['smsc']+"','SUBMITTED',0)"
            #     else:
            #         subsmsquery="update submitted_report_V2 set smscId=CONCAT(smscId,',','"+subsmsdict['smscId']+"') where msgId='"+subsmsdict['msgId']+"'"

            #     subsmsmysql.update_data(subsmsquery)
            #     logging.info(subsmsquery)

            # else:

            #     ### Batch Insert Code Block of Non Multipart Messages
            #     subsmsexecutemanyquery="insert into sms_submitted_report_V2 (msgId,subTime,smscId,smsc,status,errcode) "+\
            #     " VALUES(%s,%s,%s,%s,'SUBMITTED',0)"

            #     subsmsrow=(subsmsdict['msgId'],subsmsdict['subTime'],subsmsdict['smscId'],subsmsdict['smsc'])

            #     if subsms_db_values is None:
            #         subsms_db_values=[subsmsrow]
            #     else:
            #         subsms_db_values.append(subsmsrow)

            #     if len(subsms_db_values)==INSERT_MANY_RECORD_COUNT:
            #         logging.info('Query:%s, Values:%s',subsmsexecutemanyquery,subsms_db_values)
            #         # logging.info(subsms_db_values)

            #         subsmsmysql.execute_many(subsmsexecutemanyquery,subsms_db_values)
            #         subsms_db_values=None

        except Exception as e:
            logging.error('Exception :%s',str(e))

def deliveredMessage2MySQL():

    # dlrrptmysql=MySQLDB()

    # Initialize consumer variable
    dlrrptconsumer = KafkaConsumer (DELIVER_SMS_TOPIC, bootstrap_servers =[KAFKA_BOOTSTRAP_SERVER])

    # Read message from consumer
    for msg in dlrrptconsumer:

        # if True:
        try:
            #  Encode ASCII to Bytes encode('iso-8859-15')
            #  Decode Bytes into UTF-8 Format decode("utf-8")

            delivery_sm=json.loads(msg.value.decode("utf-8"))
            logging.info("Topic Name=%s,Message=%s"%(msg.topic,delivery_sm))
            ####### FOR BULK INSERT INTO SQL LOG FILE ################################
            rcvsmsinst=f"insert ignore into sms_delivered_report_V2 (deliverTime,smscId,receipt,status,errcode)VALUES('{delivery_sm['msgdict']['done date'] }','{delivery_sm['msgdict']['id']}','{delivery_sm['Message']}','{delivery_sm['msgdict']['stat']}','{delivery_sm['msgdict']['err']}' );\n"
            delivered_report(rcvsmsinst,INSERT_QUERIES_PATH)
            ##########################################################################
            # query="select msgId from sms_submitted_report_V2 where smscId like '%"+delivery_sm['msgdict']['id']+"%'"

            # logging.info(query)
            # rows=dlrrptmysql.fetch_data(query)

            # msgId=None

            # if len(rows)>0:
            #     for row in rows:
            #         msgId=row['msgId']

            #         messagestr=delivery_sm['Message']
            #         try:
            #             if len(delivery_sm['msgdict']['done date'])==0:
            #                     delivery_sm['msgdict']['done date']=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        
                        
                        
            #             dlrrptquery="insert into sms_delivered_report_V2 (msgId,deliverTime,smscId,receipt,status,errcode) "+\
			# 	    " VALUES('"+msgId+"','"+ delivery_sm['msgdict']['done date'] +"','"+delivery_sm['msgdict']['id']+ \
            #                         "','"+messagestr+"','"+delivery_sm['msgdict']['stat']+"','"+delivery_sm['msgdict']['err']+"' )"
			
            #             logging.info(dlrrptquery)
            #             dlrrptmysql.update_data(dlrrptquery)
            #         except Exception as e:
            #             logging.error('Exception :%s',str(e))
            #             dlrrptquery="update sms_delivered_report_V2 set smscId=CONCAT(smscId,',','"+delivery_sm['msgdict']['id']+"'),receipt=CONCAT(receipt,',','"+messagestr+"') where msgId='"+msgId+"'"
            #             logging.info(dlrrptquery)
            #             dlrrptmysql.update_data(dlrrptquery)
            # else:
            #     logging.info('DLR MSGID Not Found')

        except Exception as e:
            logging.error('Exception :%s',str(e))


t1 = threading.Thread(target=receivedMessage2MySQL, name='t1')
t1.start()

t2 = threading.Thread(target=submittedMessage2MySQL, name='t2')
t2.start()

t3 = threading.Thread(target=deliveredMessage2MySQL, name='t3')
t3.start()

# Terminate the script
sys.exit()
