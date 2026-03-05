""" SMPP Transreceiver Cliett"""

import logging
# import sys

import json

from datetime import datetime, date
from os import path,makedirs

import configparser
from kafka import KafkaProducer
from sqlbulkfile import insert_report,submitted_report,delivered_report
from utility import *
import smpplib.gsm
import smpplib.client
import smpplib.consts
import redis
from dotenv import load_dotenv
import os

# Load environment variables from .env
load_dotenv()

# Environment variables

KAFKA_BOOTSTRAP_SERVER=os.getenv('KAFKA_BOOTSTRAP_SERVER')
IS_PREPAID=int(os.getenv('IS_PREPAID'))

#DELIVER_SMS_TOPIC=os.getenv('DELIVER_SMS_TOPIC')
SMPP_RX_SERVER_IP=os.getenv('SMPP_RX_SERVER_IP')
SMPP_RX_PORT=int(os.getenv('SMPP_RX_PORT'))
SMPP_RX_USER=os.getenv('SMPP_RX_USER')
SMPP_RX_PASSWORD=os.getenv('SMPP_RX_PASSWORD')
LOGS_PATH=os.getenv('LOGS_PATH')
DELIVERY_CDR=os.getenv('DELIVERY_CDR')
REDIS_HOST = os.getenv("REDIS_HOST")
REDIS_PORT = int(os.getenv("REDIS_PORT"))


# logdirectory='./logs/'+date.today().strftime("%Y-%m-%d")+'/'
logdirectory=LOGS_PATH

if not path.exists(logdirectory):
    makedirs(logdirectory)
    print("Directory " , logdirectory ,  " Created ")
else:
    print("Directory " , logdirectory ,  " already exists")

logging.basicConfig(filename=logdirectory+'/SMPP-RX-Client.log', filemode='a',level=logging.INFO, format='%(asctime)s - %(levelname)s - %(funcName)s - %(message)s')

producer = KafkaProducer(bootstrap_servers=[KAFKA_BOOTSTRAP_SERVER], value_serializer=lambda x: json.dumps(x).encode('utf-8'))

def decode_dlr_message(msg):
    """ Decode Delivery Report Message Part """

    dlrdict={}
    msg=msg.strip()

    keys=['id:','sub:','dlvrd:','submit date:','done date:','stat:','err:','text:']
    for key in keys:
        keyloc=msg.find(key)
        strpart=msg[keyloc:]
        if key=='text:':
            value=strpart[strpart.find(':'):].strip().replace(':','')
        else:
            value=strpart[strpart.find(':'):].split(' ')[0].strip().replace(':','')

        dlrdict[key.replace(':','')]=value

    return dlrdict


def handle_deliver_sm(pdu):
    """ Handle delivery receipts (and any MO SMS) """

    delivery_sm={}

    try:
        logging.info(pdu.short_message.decode('UTF-8'))
        dlrrptdict=decode_dlr_message(pdu.short_message.decode('UTF-8'))

        # dlrrptdict['submit date']=datetime.strptime(dlrrptdict['submit date'], '%y%m%d%H%M%S').strftime('%Y-%m-%d %H:%M:%S')
        # dlrrptdict['done date']=datetime.strptime(dlrrptdict['done date'], '%y%m%d%H%M%S').strftime('%Y-%m-%d %H:%M:%S')

        delivery_sm['DCS']=pdu.data_coding
        delivery_sm['Source']=pdu.source_addr.decode('UTF-8')
        delivery_sm['Destination']=pdu.destination_addr.decode('UTF-8')
        delivery_sm['Message']=pdu.short_message.decode('UTF-8')
        delivery_sm['Delivered']=pdu.receipted_message_id
        delivery_sm['msgdict']=dlrrptdict

        logging.info(delivery_sm)

        #### CHECKING SMSCID IN REDIS
        if(IS_PREPAID==1):
            redis_smsc = check_smsid_exists(delivery_sm['msgdict']['id'])
            if redis_smsc == True:
                print("SMSC ID EXIST FROM REDIS",int(delivery_sm['msgdict']['err']))
                #create_sms_delivered(delivery_sm['msgdict']['id'],delivery_sm['msgdict']['stat'],delivery_sm['msgdict']['done date'])
                client=get_clientid_from_smsid(delivery_sm['msgdict']['id'])
                if int(delivery_sm['msgdict'].get('err')) in (None, '0', 0):
                    deduct_balance(client['clientid'],int(client['parts']))
                delete_record_submit("sms_insert",client['msgid'])
                delete_record_submit("sms_submit",delivery_sm['msgdict']['id'])
            

        print("********** DELIVER_SMS_TOPIC ************")
        #rcvsmsdelvy=f"insert ignore into sms_delivered_report_V2 (deliverTime,smscId,receipt,status,errcode)VALUES('{delivery_sm['msgdict']['done date'] }','{delivery_sm['msgdict']['id']}','{delivery_sm['Message']}','{delivery_sm['msgdict']['stat']}','{delivery_sm['msgdict']['err']}' );\n"
        rcvsmsdelvy=f"{delivery_sm['msgdict']['done date'] }|{delivery_sm['msgdict']['id']}|{delivery_sm['Message']}|{delivery_sm['msgdict']['stat']}|{delivery_sm['msgdict']['err']}\n"

        print(rcvsmsdelvy)
        delivered_report(rcvsmsdelvy,DELIVERY_CDR)
        print("*****************************************")

        ### push message to kafka dlrrpt queue
        # producer.send(DELIVER_SMS_TOPIC, value=delivery_sm)
        # producer.flush()
    except Exception as exception:
        logging.info('Exception in handle_deliver_sm:%s',str(exception))

    return 0 # cmd status for deliver_sm_resp

client = smpplib.client.Client(SMPP_RX_SERVER_IP, SMPP_RX_PORT,SMPP_RX_USER,SMPP_RX_PASSWORD,'RX')

client.set_message_received_handler(lambda pdu: handle_deliver_sm(pdu))

client.connect()
client.bind_receiver(system_id=SMPP_RX_USER, password=SMPP_RX_PASSWORD)

# Enters a loop, waiting for incoming PDUs
client.listen()

# Terminate the script
# sys.exit()
