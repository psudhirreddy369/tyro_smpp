from datetime import datetime

ts = "260309113011"

dt = datetime.strptime(str(ts), "%y%m%d%H%M%S")

formatted = dt.strftime("%Y-%m-%d %H:%M:%S")

print(formatted)