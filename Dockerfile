FROM python:3.9

# Set timezone to IST
RUN apt-get update && apt-get install -y tzdata
ENV TZ=Asia/Kolkata

# 
WORKDIR /code

# 
COPY ./requirements.txt /code/requirements.txt

# 
RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt
RUN mkdir -p /code/logs
RUN mkdir -p /code/insert_smsqueries
# Set permissions for the logs directory
RUN chmod -R 777 /code/logs
RUN chmod -R 777 /code/insert_smsqueries

# 
COPY ./ /code/
RUN useradd -m myuser
USER myuser
CMD ["sh", "-c", "python3 smpp_tx_client.py & python3 smpp_rx_client.py & wait"]
#CMD python3 airtel_tx_client.py && python3 airtel_rx_client.py && python3 AirtelKafkaTopic2MySQL.py
