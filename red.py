import redis
import json

# Connect to Redis
redis_client = redis.StrictRedis(host='localhost', port=6379, decode_responses=True)

def get_kafkapipe_for_user(username):
    try:
        # Retrieve the JSON string from the Redis key
        user_data_str = redis_client.get("users_gateways")
        if user_data_str:
            # Parse the JSON string into a list of dictionaries
            user_data_list = json.loads(user_data_str)
            
            for user_entry in user_data_list:
                # Check if the username exists as a key
                if username in user_entry:
                    return user_entry[username]
        
        # If no match is found, return None
        return None
    except Exception as e:
        print(f"Error retrieving kafkapipe for user {username}: {e}")
        return None

# Example usage
username = "LH_APCPOTP"
kafkapipe = get_kafkapipe_for_user(username)

if kafkapipe:
    print(f"Kafkapipe for user '{username}': {kafkapipe}")
else:
    print(f"No kafkapipe found for user '{username}'")
