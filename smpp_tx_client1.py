""" SMPP Transmitter Client """

import logging

import json
from datetime import datetime, date

from threading import Thread
from os import path, makedirs

from time import sleep
import configparser
import hashlib
from kafka import KafkaProducer, KafkaConsumer
from sqlbulkfile import insert_report,submitted_report,delivered_report
import pandas as pd
import pytz
import smpplib.gsm
import smpplib.client
import smpplib.consts
import struct
#from mysqldb import MySQLDB


from dotenv import load_dotenv
import os

# Load environment variables from .env
load_dotenv()

# Environment variables
KAFKA_BOOTSTRAP_SERVER = os.getenv('KAFKA_BOOTSTRAP_SERVER')
RECEIVE_SMS_TOPIC = os.getenv('RECEIVE_SMS_TOPIC')
#SUBMIT_SMS_TOPIC = os.getenv('SUBMIT_SMS_TOPIC')

SMPP_TX_SERVER_IP = os.getenv('SMPP_TX_SERVER_IP')
SMPP_TX_PORT = int(os.getenv('SMPP_TX_PORT'))
SMPP_TX_USER = os.getenv('SMPP_TX_USER')
SMPP_TX_PASSWORD = os.getenv('SMPP_TX_PASSWORD')
TELEMARKETING_ID = os.getenv('TELEMARKETING_ID')
INSERT_QUERIES_PATH=os.getenv('INSERT_QUERIES_PATH')
LOGS_PATH=os.getenv('LOGS_PATH')
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

submit_df = pd.DataFrame()
tempdata= b''
producer = KafkaProducer(bootstrap_servers=[KAFKA_BOOTSTRAP_SERVER], value_serializer=lambda x: json.dumps(x).encode('utf-8'))

def generate_hash(peid: str, telemarketing_id: str) -> str:
    """
    Generate a SHA-256 hash from the combination of peid and telemarketing_id.
    """
    combined_string = peid + ","+telemarketing_id
    return hashlib.sha256(combined_string.encode("utf-8")).hexdigest()

def send_message(msg_id: str, source: str, destination: str, message_type: str, message: str, template_id: str, pe_id: str, telemarketing_id: str,encoding_flag:int,msg_type_flag:int,msg_parts:int,msg_part:int):
    """  Send Mesage via SMPP Client library """

    global submit_df
    global tempdata
    msg_part_num=1
    # Two parts, GSM default / UCS2, SMS with UDH
    if message_type=='T':
        part = message.encode('utf-8')
        
    else:
        part = message.encode('utf-16-be')
    print("**********Part *********************")
    print(part)
    print("**************************************")
    print(message)
    print("*****************Part *******************")
    if(msg_parts>1):
        tempdata=tempdata+part
    
    sequence = client.sequence_generator.sequence+1
    india_timezone = pytz.timezone("Asia/Kolkata")
    current_time_in_india = datetime.now(india_timezone)
    timestamp = current_time_in_india.strftime('%Y-%m-%d %H:%M:%S')
    submitdict = {'msgId': msg_id, 'subTime': timestamp, 'smscId': '',
                    'smsc': SMPP_TX_USER, 'status': 'SUBMITTED', 'errcode': 0, 'sequence': sequence,
                    'msgcount': 1, 'msg_part_num': msg_part_num,'msgtype': message_type}
    
    submit_df = pd.concat([submit_df, pd.DataFrame([submitdict])], ignore_index=True)
    hts_id=generate_hash(pe_id,telemarketing_id)
    hts=hts_id.encode("ASCII")
    #msg_part_num=msg_part_num+1
    logging.info(submitdict)
    td=pe_id.encode("utf-8")
    
    udh = struct.pack('!BBBBBB',
                          5,  # UDH length
                          0,  # IEI (concatenation)
                          3,  # Length of concatenation data
                          16,  # Unique reference number
                          msg_parts,  # Total parts
                          msg_part)  # Current part number
    if(msg_parts==msg_part):
        short_msgs=udh+tempdata
        tempdata= b''
    else:
        short_msgs=udh+part
    try:
        
        pdu = client.send_message(
            source_addr_ton=smpplib.consts.SMPP_TON_ALNUM,
            source_addr_npi=smpplib.consts.SMPP_NPI_UNK,
            source_addr=source,
            dest_addr_ton=smpplib.consts.SMPP_TON_INTL,
            dest_addr_npi=smpplib.consts.SMPP_NPI_ISDN,
            destination_addr=destination,
            short_message=short_msgs,

            data_coding=encoding_flag,
            #data_coding=0,
            #esm_class=msg_type_flag,
            esm_class=0x40,
            registered_delivery=True,

            template_id=template_id.encode("utf-8"),
            pe_id=td,
            telemarketing_id=hts,
            
            
            
        )
        
        
    except Exception as exception:
        print('Exception : %s',str(exception))
        logging.error('Exception : %s',str(exception))
        return 0


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
    # Read and print message from consumer
    for msg in consumer:
        # if True:
        try:
            # Encode ASCII to Bytes encode('iso-8859-15')
            # Decode Bytes to UTF-8 Format decode("utf-8")

            # if message['messagetype']=='T':
            #     hexstr=message['message'].encode("utf-8").hex()

            message = json.loads(msg.value.decode("utf-8"))
            print("**************kafka******************")
            print(message)
            print("***************kafka******************")
            # Parameters MessageID , Source , Destination,MessageType, Message,SMS Template ID, Entity ID,  Telemarketing ID
            send_message(message['messageid'], message['sender'], message['dest'], message['messagetype'], message['message'], message['templateid'], message['peid'], TELEMARKETING_ID,message['encoding_flag'],message['msg_type_flag'],message['msg_parts'],message['msg_part'])
            consumer.commit();             
            sleep(0.1)
        except Exception as exception:
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
