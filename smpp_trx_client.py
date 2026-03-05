""" SMPP Transreceiver Cliett"""

import logging

import json
from datetime import datetime, date

from threading import Thread
from os import path, makedirs

from time import sleep
import configparser
import pandas as pd

from kafka import KafkaProducer, KafkaConsumer

from mysqldb import MySQLDB

import smpplib.gsm
import smpplib.client
import smpplib.consts
import socket

config = configparser.ConfigParser()
config.read('mobismart.ini')

KAFKA_BOOTSTRAP_SERVER = config['DEFAULT']['KAFKA_BOOTSTRAP_SERVER']
RECEIVE_SMS_TOPIC = config['DEFAULT']['RECEIVE_SMS_TOPIC']
SUBMIT_SMS_TOPIC = config['DEFAULT']['SUBMIT_SMS_TOPIC']
DELIVER_SMS_TOPIC = config['DEFAULT']['DELIVER_SMS_TOPIC']
TELEMARKETING_ID = config['DEFAULT']['TELEMARKETING_ID']
SMPP_SERVER_IP = config['SMPP-TRX']['SMPP_SERVER_IP']
SMPP_PORT = int(config['SMPP-TRX']['SMPP_PORT'])
SMPP_USER = config['SMPP-TRX']['SMPP_USER']
SMPP_PASSWORD = config['SMPP-TRX']['SMPP_PASSWORD']

#logdirectory = './logs/'+date.today().strftime("%Y-%m-%d")+'/'
logdirectory='./logs/'

if not path.exists(logdirectory):
    makedirs(logdirectory)
    print("Directory ", logdirectory,  " Created ")
else:
    print("Directory ", logdirectory,  " already exists")

logging.basicConfig(filename=logdirectory+'/SMPP-TRX-Client.log', filemode='a', level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(funcName)s - %(message)s')

mysql = MySQLDB()

submit_df = pd.DataFrame()

producer = KafkaProducer(bootstrap_servers=[KAFKA_BOOTSTRAP_SERVER],
                    value_serializer=lambda x: json.dumps(x).encode('utf-8'))

def send_message(msg_id: str, source: str, destination: str, message_type: str, message: str,
                            template_id: str, pe_id: str, telemarketing_id: str):
    """  Send Mesage via SMPP Client library """

    global submit_df

    msg_part_num=1
    # Two parts, GSM default / UCS2, SMS with UDH
    parts, encoding_flag, msg_type_flag = smpplib.gsm.make_parts(message)

    for part in parts:
        sequence = client.sequence_generator.sequence+1

        submitdict = {'msgId': msg_id, 'subTime': datetime.now().strftime('%Y-%m-%d %H-%M-%S'),
                    'smscId': '', 'smsc': SMPP_USER,'status': 'SUBMITTED', 'errcode': 0, 'sequence': sequence,
                    'msgcount': len(parts), 'msg_part_num': msg_part_num,'msgtype': message_type}

        # Store the data into Pandas Dataframe (In memory data)
        # submit_df = submit_df.append(submitdict, ignore_index=True) #### OLD LINE OF CODE
        submit_df = pd.concat([submit_df, pd.DataFrame([submitdict])], ignore_index=True)
        
        msg_part_num=msg_part_num+1

        logging.info(submitdict)
        pdu = client.send_message(
        source_addr_ton=smpplib.consts.SMPP_TON_ALNUM,
        source_addr_npi=smpplib.consts.SMPP_NPI_UNK,
        source_addr=source,
        dest_addr_ton=smpplib.consts.SMPP_TON_INTL,
        dest_addr_npi=smpplib.consts.SMPP_NPI_ISDN,
        destination_addr=destination,
        short_message=part,
        data_coding=encoding_flag,
        esm_class=msg_type_flag,
        registered_delivery=True,
        template_id=template_id.encode("utf-8"),
        pe_id=pe_id.encode("utf-8"),
        telemarketing_id=telemarketing_id.encode("utf-8")
        )
    return 0


def read_messages_from_kafka_queue():
    """  Consume Messages from Kafka Topic and Send SMS """

    # Initialize consumer
    consumer = KafkaConsumer(RECEIVE_SMS_TOPIC, bootstrap_servers=[KAFKA_BOOTSTRAP_SERVER],enable_auto_commit=False,group_id='airtel')

    # Read and print message from consumer
    for msg in consumer:
        # if True:
        try:
            # Encode ASCII to Bytes encode('iso-8859-15')
            # Decode Bytes to UTF-8 Format decode("utf-8")

            # if message['messagetype']=='T':
            #     hexstr=message['message'].encode("utf-8").hex()

            message = json.loads(msg.value.decode("utf-8"))

            # Parameters MessageID,Source,Destination,MessageType,Message,SMS Template ID,Entity ID, Telemarketing ID
            send_message(message['messageid'], message['sender'], message['dest'], message['messagetype'],
                        message['message'], message['templateid'], message['peid'], TELEMARKETING_ID)
            consumer.commit()
            sleep(0)
        except Exception as exception:
            logging.error('Exception :%s',str(exception))
    return 0





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

        producer.send(SUBMIT_SMS_TOPIC, value=subsmsdict)

        # Flush the Pandas Dataframe (In memory data of matching Sequence number)
        submit_df.drop(submit_df[submit_df['sequence'] == sequence].index, inplace=True)

    except Exception as exception:
        logging.error('Exception :%s',str(exception))

    return 0


def decode_dlr_message(msg):
    """ Decode Delivery Report Message Part """

    dlrdict = {}
    msg = msg.strip()

    keys = ['id:', 'sub:', 'dlvrd:', 'submit date:', 'done date:', 'stat:', 'err:', 'text:']
    for key in keys:
        keyloc = msg.find(key)
        strpart = msg[keyloc:]
        if key == 'text:':
            value = strpart[strpart.find(':'):].strip().replace(':', '')
        else:
            value = strpart[strpart.find(':'):].split(' ')[0].strip().replace(':', '')

        dlrdict[key.replace(':', '')] = value

    return dlrdict



def handle_deliver_sm(pdu):
    """ Handle delivery receipts (and any MO SMS) """

    delivery_sm = {}

    # if True:
    try:
        
        logging.info(pdu.short_message.decode('UTF-8'))

        dlrrptdict = decode_dlr_message(pdu.short_message.decode('UTF-8'))
        
        # dlrrptdict['submit date'] = datetime.strptime(dlrrptdict['submit date'], '%y%m%d%H%M%S').strftime('%Y-%m-%d %H:%M:%S')
        # dlrrptdict['done date'] = datetime.strptime(dlrrptdict['done date'], '%y%m%d%H%M%S').strftime('%Y-%m-%d %H:%M:%S')

        delivery_sm['DCS'] = pdu.data_coding
        delivery_sm['Source'] = pdu.source_addr.decode('UTF-8')
        delivery_sm['Destination'] = pdu.destination_addr.decode('UTF-8')
        delivery_sm['Message'] = pdu.short_message.decode('UTF-8')
        delivery_sm['Delivered'] = str(pdu.receipted_message_id) #.decode('UTF-8')
        delivery_sm['msgdict'] = dlrrptdict

        logging.info(delivery_sm)

        #  push message to kafka dlrrpt queue
        producer.send(DELIVER_SMS_TOPIC, value=delivery_sm)

    except Exception as exception:
        logging.info('Exception :%s',str(exception))

    return 0  # cmd status for deliver_sm_resp
client = None
def reconnect():
    global client
    while True:
        try:
            logging.info("Reconnecting ESME..IP:"+str(SMPP_SERVER_IP)+" Port:"+str(SMPP_PORT))
            client = smpplib.client.Client(SMPP_SERVER_IP, SMPP_PORT,SMPP_USER,SMPP_PASSWORD,"TRX")
            client.set_message_sent_handler(lambda pdu: handle_submit_sm_resp(pdu))
            client.set_message_received_handler(lambda pdu: handle_deliver_sm(pdu))
            client.connect()
            if(client._socket is not None):
                client.bind_transceiver(system_id=client.system_id, password=client.password)
                break
        except Exception as ce:
            logging.info(str(ce))
        sleep(1) 
           
                
    
reconnect()
t1 = Thread(target=read_messages_from_kafka_queue)
t1.start()

t2 = Thread(target=client.listen)
t2.start()

