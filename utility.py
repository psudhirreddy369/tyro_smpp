import redis,json
from dotenv import load_dotenv
from os import path,makedirs
import logging,sys, glob,os
import string
import random
import datetime
import uuid
import time

# Load environment variables from .env
load_dotenv()

REDIS_HOST = os.getenv("REDIS_HOST")
REDIS_PORT = int(os.getenv("REDIS_PORT"))
LOGS_PATH=os.getenv('LOGS_PATH')
key = "messages_zset"
hash_key = "uuid_to_member"

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

def delete_message_when_submit_sm(uuid):
    member_str = r.hget(hash_key, uuid)
    if not member_str:
        print(f"No message found with UUID: {uuid}")
        return False

    # Remove from sorted set
    removed = r.zrem(key, member_str)
    if removed:
        # Remove from hash
        r.hdel(hash_key, uuid)
        print(f"Deleted message with UUID: {uuid}")
        return True
    else:
        print(f"Failed to delete message with UUID: {uuid}")
        return False

def store_message_for_failure_retry(message):
    unique_id = message['messageid']
    created_at = time.time()

    record = {
        "uuid": unique_id,
        "created_at": created_at,
        "data": message
    }
    member_str = json.dumps(record)

    # Add to sorted set with timestamp as score
    r.zadd(key, {member_str: created_at})
    # Map uuid to member string for O(1) deletion
    r.hset(hash_key, unique_id, member_str)

    print(f"Stored message with UUID: {unique_id}")
    return unique_id

def create_sms_submitted(msgid,smscid,datetime):
    try:
        sms_submit_data = {"msgid": msgid,"smscid": str(smscid),"datetime": datetime}
        print(sms_submit_data)
        r.hset("sms_submit", smscid, json.dumps(sms_submit_data))
        return sms_submit_data
    except Exception as e:
        #logging.error(str(e))
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)
        return {"message": str(e)}


def create_sms_delivered(smscid,status,datetime):
    try:
        sms_deliver_data = {"smsid": smscid,"status": status,"datetime": datetime}
        r.hset("sms_deliver", smscid, json.dumps(sms_deliver_data))
        print(f"Stored sms_submit for smsid {smscid} with status {status} at {datetime}")
        return sms_deliver_data
    except Exception as e:
        #logging.error(str(e))
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)
        return {"message": str(e)}

def get_clientid_from_smsid(smscid):
    try:
        # 1. Get sms_submit record by smsid
        sms_submit_raw = r.hget("sms_submit", smscid)
        if not sms_submit_raw:
            return {"error": "sms_submit record not found for smsid"}

        sms_submit_data = json.loads(sms_submit_raw)
        msgid = sms_submit_data.get("msgid")
        if not msgid:
            return {"error": "msgid missing in sms_submit record"}

        # 2. Get sms_insert record by msgid
        sms_insert_raw = r.hget("sms_insert", msgid)
        if not sms_insert_raw:
            return {"error": "sms_insert record not found for msgid"}

        sms_insert_data = json.loads(sms_insert_raw)
        clientid = sms_insert_data.get("clientid")
        parts = sms_insert_data.get("parts")
        if clientid is None:
            return {"error": "clientid missing in sms_insert record"}

        return {"clientid": clientid,"msgid":msgid,"parts":parts}
    except Exception as e:
        #logging.error(str(e))
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)
        return {"message": str(e)}


def delete_record_insert(hash_name: str, key: str):
    try:
        result = r.hdel("sms_insert", key)
        
        if result == 1:
            print(f"Successfully deleted key '{key}' from hash '{hash_name}'")
            return {"message": f"Deleted key '{key}' from '{hash_name}'"}
        else:
            print(f"Key '{key}' not found in hash '{hash_name}'")
            return {"error": f"Key '{key}' not found in '{hash_name}'"}
    except Exception as e:
        #logging.error(str(e))
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)
        return {"message": str(e)} 

def delete_record_submit(hash_name: str, key: str):
    try:
        result = r.hdel(hash_name, key)
        
        if result == 1:
            print(f"Successfully deleted key '{key}' from hash '{hash_name}'")
            return {"message": f"Deleted key '{key}' from '{hash_name}'"}
        else:
            print(f"Key '{key}' not found in hash '{hash_name}'")
            return {"error": f"Key '{key}' not found in '{hash_name}'"}
    except Exception as e:
        #logging.error(str(e))
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)
        return {"message": str(e)}           
    
    
def check_msgid_exists(msgid):
    try:
        exists = r.hexists("sms_insert",str(msgid).strip())
        if exists:
            print(f"msgid '{msgid}' exists in 'sms_insert'")
            return True
        else:
            print(f"msgid '{msgid}' not found in 'sms_insert'")
            return False
    except Exception as e:
        #logging.error(str(e))
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)
        return {"message": str(e)}
    

def check_smsid_exists(smscid):
    try:
        exists = r.hexists("sms_submit", smscid)
        if exists:
            print(f"smscid '{smscid}' exists in 'sms_submit'")
            return True
        else:
            print(f"smscid '{smscid}' not found in 'sms_submit'")
            return False
    except Exception as e:
        #logging.error(str(e))
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)
        return {"message": str(e)}

# DEDUCT BALANCE FROM REDIS
def deduct_balance(clientid_redis, number_of_sms):
    try:
        print("The client is ", clientid_redis)
        print("The count to debut is ", number_of_sms)
        user_data = r.hget('prepaid_users', int(clientid_redis))
        if user_data:
            user = json.loads(user_data)
            sms_pulse = float(user.get('smspulse', 0.0))
            #smspulse = int(sms_pulse)/100
            current_balance = float(user.get('smsbalance', 0.0))
            # Deduct balance
            deduction = number_of_sms * sms_pulse # (1*30=30)
            result = current_balance - deduction
            user['smsbalance'] = max(0, round(result, 1))
            user['smscredits'] = int(user['smscredits'])-number_of_sms
            # Update the user record in Redis
            r.hset('prepaid_users', int(clientid_redis), json.dumps(user))
            print(f"Updated balance for {clientid_redis}: {user['smsbalance']}")
        else:
            print(f"User {clientid_redis} not found")
    except Exception as e:
        #logging.error(str(e))
        print(str(e))
        return {"message": str(e)}        