from datetime import datetime
import pytz


india_timezone = pytz.timezone("Asia/Kolkata")
current_time_in_india = datetime.now(india_timezone)
timestamp = current_time_in_india.strftime('%Y-%m-%d %H:%M:%S')
print(timestamp)