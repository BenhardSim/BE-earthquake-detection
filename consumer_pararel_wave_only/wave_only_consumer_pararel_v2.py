from confluent_kafka import Consumer, KafkaError
import tensorflow as tf
from tensorflow import keras
import multiprocessing
import certifi
import uuid
import configparser
from datetime import datetime, timedelta
import json
import numpy as np
import logging
import firebase_admin
from firebase_admin import db, credentials

unique_group_id = f'my-consumer-group-{str(uuid.uuid4())}'

cred = credentials.Certificate('credentials.json')
firebase_admin.initialize_app(cred , 
                              {"databaseURL" : "https://eews-pipeline-default-rtdb.asia-southeast1.firebasedatabase.app/"})




# Create a ConfigParser instance
config = configparser.ConfigParser()
# Load the configuration from the file
config.read('kafka_config.txt')

bootstrap_servers = config.get('KafkaConfig', 'bootstrap.servers')
sasl_username = config.get('KafkaConfig', 'sasl.username')
sasl_password = config.get('KafkaConfig', 'sasl.password')

# config settings
kafka_config = {
    'bootstrap.servers': bootstrap_servers, 
    'sasl.mechanism': 'PLAIN',
    'security.protocol': 'SASL_SSL',
    'sasl.username': sasl_username,
    'sasl.password': sasl_password,
    "ssl.ca.location": certifi.where(),
    'linger.ms': 100,
    'group.id': unique_group_id,  # Specify a unique consumer group ID
    'auto.offset.reset': 'latest'
}

# connect ke confluen kafka
consumer = Consumer(kafka_config)

# mensbscribe kafka topic 
kafka_topic = config.get('KafkaConfig', 'kafka.topic') 
consumer.subscribe([kafka_topic])

def log_data(data, type="LOG",stat="-"):
    print(f"[+] <{stat}> <{type}>  {data}")

def prediction_res(data_responses):
    converter_np_array = np.zeros((0, 3))

    len_BHE = len(data_responses["BHE"])
    len_BHN = len(data_responses["BHN"])
    len_BHZ = len(data_responses["BHZ"])

    min_len_channel = min(len_BHE,min(len_BHZ,len_BHN))
    data_start_time = data_responses["start_time"]
    utc_datetime_start_time = datetime.strptime(data_start_time, "%Y-%m-%dT%H:%M:%S.%fZ")
    data_waves = {}

    for i in range(20*4,min_len_channel):
        BHE_channel = data_responses["BHE"][i]
        BHN_channel = data_responses["BHN"][i]
        BHZ_channel = data_responses["BHZ"][i]
        # index = data_responses["start_time"] + (0.05*i)
        time_interval = timedelta(microseconds=i*50000)
        waves_timestamp = utc_datetime_start_time + time_interval
        waves_timestamp_string = waves_timestamp.strftime("%Y-%m-%d %H:%M:%S:%f")
        data_waves[waves_timestamp_string] = {
            "E" : BHE_channel,
            "N" : BHN_channel,
            "Z" : BHZ_channel
        }
        data_row = np.array([BHE_channel,BHN_channel,BHZ_channel])
        converter_np_array = np.vstack((converter_np_array, data_row))

    print(f"Result {data_responses['stat']}...")
    log_data(stat="LEN DATA",data=f'LEN : {min_len_channel}') 
    log_data(stat="DATA ID",data=f'ID : {data_responses["id"]}') 
    log_data(stat="TIME",data=f'start time : {data_responses["start_time"]}, end time : {data_responses["end_time"]}') 
    log_data(data=f'Shortest Channel length : {min_len_channel}', stat="Min Channel")
    result_dict = {}
    data_waves["stat"] = data_responses['stat']
    result_dict["stat"] = data_responses['stat']
    result_dict["data"] = converter_np_array
    return data_waves

if __name__ == "__main__":
    num_processes = 1
    consumer = Consumer(kafka_config)
    consumer.subscribe([kafka_topic])

    pool = multiprocessing.Pool(processes=num_processes)

    while True:
        # ref = db.reference("/waves")
        ref_JAGI = db.reference("/waves/JAGI")
        ref_SMRI = db.reference("/waves/SMRI")
        ref_BBJI = db.reference("/waves/BBJI")
        messages = []  # Collect Kafka messages to process in parallel

        # Collect a batch of messages
        while len(messages) < num_processes:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print(f'Reached end of partition for topic {msg.topic()} [{msg.partition()}]')
                else:
                    print(f'Error while consuming message: {msg.error()}')
            else:
                json_msg = json.loads(msg.value().decode('utf-8'))
                # partition_key = msg.key().decode('utf-8')
                messages.append(json_msg)

        if messages:
            # Process the received messages in parallel using pool.map
            wave_result = pool.map(prediction_res, messages)

            try:
                json_string = json.dumps(wave_result)
                # print(wave_result[0])
                # print(wave_result[1])
                # print(wave_result[2])
                # Use the push method to add data (creates a new unique key)
                # print(wave_result[0])
                # print(wave_result[1])
                # print(wave_result[2])

                for index, predic_res in enumerate(wave_result):
                    if wave_result[index]["stat"] == "JAGI":
                        data_wave = wave_result[index]
                        new_record_ref = ref_JAGI.push(data_wave)
                        print("Data pushed to Database JAGI successfully with key:", new_record_ref.key)
                    elif wave_result[index]["stat"] == "SMRI":
                        data_wave = wave_result[index]
                        new_record_ref = ref_SMRI.push(data_wave)
                        print("Data pushed to Database SMRI successfully with key:", new_record_ref.key)
                    elif wave_result[index]["stat"] == "BBJI":
                        data_wave = wave_result[index]
                        new_record_ref = ref_BBJI.push(data_wave)
                        print("Data pushed to Database BBJI successfully with key:", new_record_ref.key)

            except Exception as e:
                print("Error pushing data:", e)
