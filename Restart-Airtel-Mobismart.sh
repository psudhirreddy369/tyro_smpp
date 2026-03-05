#ps aux | grep -i AirtelMobismartRESTAPI | grep -v grep | awk {'print $2'} | xargs kill -9

#nohup gunicorn AirtelMobismartRESTAPI:app -w 4 -k uvicorn.workers.UvicornWorker -b 0.0.0.0:8002 --daemon > /dev/null 2>&1 &

#nohup gunicorn AirtelMobismartRESTAPI:app -w 4 -k uvicorn.workers.UvicornWorker --certfile=crt_files/certificate.crt --keyfile=crt_files/private.key  --ssl-version=TLSv1_2 -b 0.0.0.0:8002 --daemon > /dev/null 2>&1 &
#sleep 5

# ps aux | grep -i airtel_trx_client | grep -v grep | awk {'print $2'} | xargs kill -9
# nohup python3 airtel_trx_client.py -daemon > /dev/null 2>&1 &
# sleep 2

ps aux | grep -i smpp_tx_client | grep -v grep | awk {'print $2'} | xargs kill -9
nohup python3 smpp_tx_client.py -daemon > /dev/null 2>&1 &
sleep 2

ps aux | grep -i smpp_rx_client | grep -v grep | awk {'print $2'} | xargs kill -9
nohup python3 smpp_rx_client.py -daemon > /dev/null 2>&1 &
sleep 2

#ps aux | grep -i AirtelKafkaTopic2MySQL.py | grep -v grep | awk {'print $2'} | xargs kill -9
#nohup python3 AirtelKafkaTopic2MySQL.py -daemon > /dev/null 2>&1 & 
#sleep 2
