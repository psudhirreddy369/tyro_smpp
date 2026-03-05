""" SMPP Transmitter Client """

import logging

import json
from datetime import datetime, date

from threading import Thread
from os import path, makedirs

from time import sleep
import configparser
import hashlib
from kafka import KafkaProducer, KafkaConsumer,TopicPartition
from sqlbulkfile import insert_report,submitted_report,delivered_report
from utility import create_sms_submitted,check_msgid_exists,store_message_for_failure_retry,delete_message_when_submit_sm
import pandas as pd
import pytz
import smpplib.gsm
import smpplib.client
import smpplib.consts
import traceback
#from mysqldb import MySQLDB
import struct

from dotenv import load_dotenv
import os
from datetime import datetime
import threading
from collections import deque
# Load environment variables from .env
load_dotenv()

# Environment variables
KAFKA_BOOTSTRAP_SERVER = os.getenv('KAFKA_BOOTSTRAP_SERVER')
RECEIVE_SMS_TOPIC = os.getenv('RECEIVE_SMS_TOPIC')
IS_PREPAID=int(os.getenv('IS_PREPAID'))
#SUBMIT_SMS_TOPIC = os.getenv('SUBMIT_SMS_TOPIC')

SMPP_TX_SERVER_IP = os.getenv('SMPP_TX_SERVER_IP')
SMPP_TX_PORT = int(os.getenv('SMPP_TX_PORT'))
SMPP_TX_USER = os.getenv('SMPP_TX_USER')
SMPP_TX_PASSWORD = os.getenv('SMPP_TX_PASSWORD')
TELEMARKETING_ID = os.getenv('TELEMARKETING_ID')
INSERT_QUERIES_PATH=os.getenv('INSERT_QUERIES_PATH')
LOGS_PATH=os.getenv('LOGS_PATH')
REDIS_HOST = os.getenv("REDIS_HOST")
REDIS_PORT = int(os.getenv("REDIS_PORT"))
print(REDIS_PORT)
#logdirectory = './logs/'+date.today().strftime("%Y-%m-%d")+'/'
logdirectory=LOGS_PATH

if not path.exists(logdirectory):
    makedirs(logdirectory)
    print("Directory ", logdirectory,  " Created ")
else:
    print("Directory ", logdirectory,  " already exists")

logging.basicConfig(filename=logdirectory+'/SMPP-TX-Client.log', filemode='a',
                    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(funcName)s - %(message)s')

#mysql = MySQLDB()
deferred_queue = deque()
deferred_lock = threading.Lock()
submit_df = pd.DataFrame()

producer = KafkaProducer(bootstrap_servers=[KAFKA_BOOTSTRAP_SERVER], value_serializer=lambda x: json.dumps(x).encode('utf-8'))

def generate_hash(peid: str, telemarketing_id: str) -> str:
    """
    Generate a SHA-256 hash from the combination of peid and telemarketing_id.
    """
    combined_string = peid + ","+telemarketing_id
    print("Generating has for :",combined_string)
    return hashlib.sha256(combined_string.encode("utf-8")).hexdigest()

def send_message(msg_id: str, source: str, destination: str, message_type: str, message: str, template_id: str, pe_id: str, telemarketing_id: str,encoding_flag:int,msg_type_flag:int,msg_parts:int,msg_part:int,unique_ref:int,ttelemarketing_id:str):
    """  Send Mesage via SMPP Client library """

    global submit_df
    
    msg_part_num=1
    # Two parts, GSM default / UCS2, SMS with UDH
    if message_type=='T':
        part = message.encode('utf-8')
    else:
        part = message.encode('utf-16-be')
    sequence = client.sequence_generator.sequence+1
    india_timezone = pytz.timezone("Asia/Kolkata")
    current_time_in_india = datetime.now(india_timezone)
    timestamp = current_time_in_india.strftime('%Y-%m-%d %H:%M:%S')
    submitdict = {'msgId': msg_id, 'subTime': timestamp, 'smscId': '',
                    'smsc': SMPP_TX_USER, 'status': 'SUBMITTED', 'errcode': 0, 'sequence': sequence,
                    'msgcount': 1, 'msg_part_num': msg_part_num,'msgtype': message_type}
    submit_df = pd.concat([submit_df, pd.DataFrame([submitdict])], ignore_index=True)
    if(ttelemarketing_id!=""):
        hts_id=ttelemarketing_id
    else:
        hts_id=generate_hash(pe_id,TELEMARKETING_ID)
    #hts_id=generate_hash(pe_id,telemarketing_id)
    hts=hts_id.encode("ASCII")
    #msg_part_num=msg_part_num+1
    logging.info(submitdict)
    td=pe_id.encode("utf-8")
    try:
        if(msg_parts==1):
            parts=part
            pdu = client.send_message(
                source_addr_ton=smpplib.consts.SMPP_TON_ALNUM,
                source_addr_npi=smpplib.consts.SMPP_NPI_UNK,
                source_addr=source,
                dest_addr_ton=smpplib.consts.SMPP_TON_INTL,
                dest_addr_npi=smpplib.consts.SMPP_NPI_ISDN,
                destination_addr=destination,
                short_message=parts,
                data_coding=0,
                esm_class=msg_type_flag,
                #esm_class=64,
                registered_delivery=True,

                template_id=template_id.encode("utf-8"),
                pe_id=td,
                telemarketing_id=td,
                
            )
        else:
            udh = struct.pack('!BBBBBB',
                          5,  # UDH length
                          0,  # IEI (concatenation)
                          3,  # Length of concatenation data
                          unique_ref,  # Unique reference number
                          msg_parts,  # Total parts
                          msg_part)  # Current part number
            
            parts=udh+part
            print(parts)
            pdu = client.send_message(
                source_addr_ton=smpplib.consts.SMPP_TON_ALNUM,
                source_addr_npi=smpplib.consts.SMPP_NPI_UNK,
                source_addr=source,
                dest_addr_ton=smpplib.consts.SMPP_TON_INTL,
                dest_addr_npi=smpplib.consts.SMPP_NPI_ISDN,
                destination_addr=destination,
                short_message=parts,
                data_coding=0,
                #esm_class=msg_type_flag,
                esm_class=64,
                registered_delivery=True,

                template_id=template_id.encode("utf-8"),
                pe_id=td,
                telemarketing_id=td,
                
            )
        return pdu.sequence
    except Exception as exception:
        print(f'Exception occurred: {exception}')
        traceback.print_exc()
        print("stst")
        logging.error('Exception : %s',str(exception))
        return 0

def deferred_worker(consumer):
    date_format = "%Y-%m-%d %H:%M:%S"
    india_timezone = pytz.timezone("Asia/Kolkata")

    while True:
        item = None
        with deferred_lock:
            if deferred_queue:
                item = deferred_queue.popleft()

        if item is None:
            sleep(1)  # No deferred messages; sleep briefly
            continue

        tp, offset, message, last_resp_connection = item
        try:
            print(f"[Deferred Worker] Reprocessing offset {offset} from {tp}")
            # Ensure partition assignment
            if tp not in consumer.assignment():
                consumer.assign([tp])
                print(f"[Worker] Assigned partition: {tp}")

            # Seek to the offset
            consumer.seek(tp, offset)
            records = consumer.poll(timeout_ms=1000)
            for _, msgs in records.items():
                for msg in msgs:
                    msg_value = json.loads(msg.value.decode("utf-8"))
                    
                    try:
                        ct = datetime.now(india_timezone)
                        diff_seconds = abs((datetime.strptime(ct.strftime('%Y-%m-%d %H:%M:%S'),date_format)- datetime.strptime(client.last_resp_connection,date_format)).total_seconds())
                        if int(diff_seconds) <= 5:
                            print("[Deferred Worker] Client active, sending message...")
                            store_message_for_failure_retry(msg_value)
                            send_message(
                                msg_value['messageid'], msg_value['sender'], msg_value['dest'],
                                msg_value['messagetype'], msg_value['message'], msg_value['templateid'],
                                msg_value['peid'], TELEMARKETING_ID, msg_value['encoding_flag'],
                                msg_value['msg_type_flag'], msg_value['msg_parts'], msg_value['msg_part'],
                                msg_value['unique_ref'], msg_value.get('telemarketing_id', '')
                            )
                            consumer.commit()
                        else:
                            print("[Deferred Worker] Client still inactive, deferring again")
                            # Put back to queue for later retry
                            with deferred_lock:
                                deferred_queue.append((tp, offset, message, last_resp_connection))
                            sleep(30)  # backoff before retrying again
                    except Exception as e:
                        print("Exception in deffered worker",str(e))

        except Exception as e:
            logging.error(f"Deferred worker exception: {e}")

def read_messages_from_kafka_queue():
    """  Consume Messages from Kafka Topic and Send SMS """
    # Initialize consumer
    #consumer = KafkaConsumer(RECEIVE_SMS_TOPIC, bootstrap_servers=[KAFKA_BOOTSTRAP_SERVER])
    consumer = KafkaConsumer(
    RECEIVE_SMS_TOPIC,
    bootstrap_servers=[KAFKA_BOOTSTRAP_SERVER],
    group_id='earliest',  # Assign the consumer to a group
    auto_offset_reset='earliest',  # Start from the beginning if no offsets are committed
    enable_auto_commit=False     # Disable auto commit to manage offsets manually
    )
    threading.Thread(target=deferred_worker, args=(consumer,), daemon=True).start()

    # Read and print message from consumer
    #message = {"user": "testing2", "password": "testing", "peid": "1201159232579916896", "templateid": "1507161554119445387", "sender": "OBHSAP", "dest": "9553374468", "messagetype": "T", "message": "Generated Feedback Verification Code {#var#} Thanks SWACHH SAFAR Portal. OBHSAP", "deliveryreport": "yes", "inTime": "2025-05-15 18:27:13", "status": "Accepted", "messageid": "9946388c41", "telemarketing_id": "62d089de39942a733a3cdbe484c0915e66802c7f191d8ea67b5e757de5d15945", "encoding_flag": 0, "msg_type_flag": 0, "msg_parts": 1, "msg_part": 1, "unique_ref": 125}
    #send_message(message['messageid'], message['sender'], message['dest'], message['messagetype'], message['message'], message['templateid'], message['peid'], TELEMARKETING_ID,message['encoding_flag'],message['msg_type_flag'],message['msg_parts'],message['msg_part'],message['unique_ref'],message['telemarketing_id'])
    for msg in consumer:
        # if True:
        try:
            message = json.loads(msg.value.decode("utf-8"))
            print("**************kafka******************")
            print(message)
            print("***************kafka******************")
            message['telemarketing_id'] = message.get('telemarketing_id', '')
            message['topic']=RECEIVE_SMS_TOPIC
            
            date_format = "%Y-%m-%d %H:%M:%S"
            india_timezone = pytz.timezone("Asia/Kolkata")
            current_time_in_india = datetime.now(india_timezone)
            diff_seconds = abs((datetime.strptime(current_time_in_india.strftime('%Y-%m-%d %H:%M:%S'),date_format)- datetime.strptime(client.last_resp_connection,date_format)).total_seconds())
            if(int(diff_seconds)<=5):
                print("Client Active")
                send_message(message['messageid'], message['sender'], message['dest'], message['messagetype'], message['message'], message['templateid'], message['peid'], TELEMARKETING_ID,message['encoding_flag'],message['msg_type_flag'],message['msg_parts'],message['msg_part'],message['unique_ref'],message['telemarketing_id'])
                store_message_for_failure_retry(message)
                consumer.commit()
                sleep(0.1)
            else:
                logging.error("SMSC is Inactive: Uncommitting record")
                print("Client Inactive")
                tp = TopicPartition(msg.topic, msg.partition)
                with deferred_lock:
                    deferred_queue.append((tp, msg.offset, message, client.last_resp_connection))
                sleep(30)
            # Parameters MessageID , Source , Destination,MessageType, Message,SMS Template ID, Entity ID,  Telemarketing ID
                         
            
        except Exception as exception:
            print('Exception :%s',str(exception))
            logging.error('Exception :%s',str(exception))
        #consumer.commit()
    return 0


client = smpplib.client.Client(SMPP_TX_SERVER_IP, SMPP_TX_PORT,SMPP_TX_USER,SMPP_TX_PASSWORD,'TX')


def handle_submit_sm_resp(pdu):
    """ Handle Submit SM Response PDU """

    global submit_df

    # if True:
    try:

        sequence = pdu.sequence
        smsc_id = pdu.message_id.decode('UTF-8')

        seq_mapped_row = submit_df[submit_df['sequence'] == sequence]

        subsmsdict = seq_mapped_row.iloc[0].to_dict()

        subsmsdict['smscId'] = smsc_id

        logging.info(subsmsdict)
        # TO CHECK IN REDIS
        print("&&&&&&&&&& REIDS CHECKING &&&&&&&&&&&")
        delete_message_when_submit_sm(subsmsdict['msgId'])
        if (IS_PREPAID==1):
            msgid_res = check_msgid_exists(subsmsdict['msgId'])
            if msgid_res == True:
                create_sms_submitted(subsmsdict['msgId'],subsmsdict['smscId'],subsmsdict['subTime'])
        print("********** SUBMIT_SMS_TOPIC ************")
        print(subsmsdict)
        rcvsmssubmit=f"insert ignore into sms_submitted_report_V2 (msgId,subTime,smscId,smsc,status,errcode)VALUES('{subsmsdict['msgId']}','{subsmsdict['subTime']}','{subsmsdict['smscId']}','{subsmsdict['smsc']}','{subsmsdict['status']}','{subsmsdict['errcode']}');\n"
        print(rcvsmssubmit)
        submitted_report(rcvsmssubmit,INSERT_QUERIES_PATH)
        print("*****************************************")

        # producer.send(SUBMIT_SMS_TOPIC, value=subsmsdict)
        # producer.flush()
        # Flush the Pandas Dataframe (In memory data of matching Sequence number)
        submit_df.drop(submit_df[submit_df['sequence'] == sequence].index, inplace=True)

    except Exception as exception:
        logging.error('Exception : %s',str(exception))

    return 0



client.set_message_sent_handler(lambda pdu: handle_submit_sm_resp(pdu))

client.connect()
client.bind_transmitter(system_id=SMPP_TX_USER, password=SMPP_TX_PASSWORD)


t1 = Thread(target=read_messages_from_kafka_queue)
t1.start()

t2 = Thread(target=client.listen)
t2.start()

# Terminate the script
# sys.exit()
