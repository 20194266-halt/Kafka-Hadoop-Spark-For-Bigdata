import sys
from kafka import KafkaConsumer
import base64
import re
import numpy as np
import pandas as pd
from hdfs import InsecureClient
import csv
import requests
from pywebhdfs.webhdfs import PyWebHdfsClient


def save_file_to_hdfs(tmp_hdfs_path):
  client = InsecureClient('http://localhost:9870', user='root')
  client.upload('/data/' + tmp_hdfs_path, tmp_hdfs_path)

def main():
  
  try:
    topic = 'MyTopicDemo'
  except Exception as ex:
    print("Failed to set topic")

  consumer = get_kafka_consumer(topic)
  subscribe(consumer)
  
  #ave_file_to_hdfs()
  
def convert_csv(arr, row, cpname):
  array = np.array(arr)
  npArray = array.reshape(row, 5)
  DF = pd.DataFrame(npArray)
  DF.to_csv(cpname + '.csv')
  save_file_to_hdfs(cpname+'.csv')

def preProcessing(msg):
  flag = False
  k = 0
  financeData = []
  message = msg.split('$')
  cpname = message[0]
  message.pop(0)
  for m in message:
    if(m.strip() != ''):
      if(re.search('[a-zA-Z]', m)):
        if(flag):
          financeData = financeData + ["not found"]*(4-k)
        financeData.append(m)
        k = 0
      else:
        flag = True
        k = k+1
        financeData.append(int(m.replace(',', '')))
  print(financeData)
  while(len(financeData)%5):
    financeData.append(0)
  financeData[4] = financeData[3]
  financeData[3] = financeData[2]
  financeData[2] = financeData[1]
  financeData[1] = financeData[0]
  convert_csv(financeData, int(len(financeData)/5), cpname)



def subscribe(consumer_instance):
    try:
        for event in consumer_instance:
            value = event.value.decode("utf-8")
            print(f"Message Received: ({value})")
        preProcessing(value)
        consumer_instance.close()
    except Exception as ex:
        print('Exception in subscribing')
        print(str(ex))

def get_kafka_consumer(topic_name, servers=['localhost:9092']):
    _consumer = None
    try:
        _consumer = KafkaConsumer(topic_name, auto_offset_reset='earliest', bootstrap_servers=servers, api_version=(0, 10), consumer_timeout_ms=10000)
    except Exception as ex:
        print('Exception while connecting Kafka')
        print(str(ex))
    finally:
        return _consumer

if __name__ == "__main__":
  main()