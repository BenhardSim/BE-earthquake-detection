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
import time
import random

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

# Hyper Parameter
WINDOW_SIZE = 80
# Machine Learning Model
model = keras.models.load_model('./tensorflow_model/window_4second_v2.h5')

# connect ke confluen kafka
consumer = Consumer(kafka_config)

# mensbscribe kafka topic 
kafka_topic = config.get('KafkaConfig', 'kafka.topic') 
consumer.subscribe([kafka_topic])

def log_data(data, type="LOG",stat="-"):
    print(f"[+] <{stat}> <{type}>  {data}")

# Check if CUDA GPU is available
if tf.test.is_gpu_available():
    print("CUDA GPU is available.")
    # Additional GPU information
    gpu_device_name = tf.test.gpu_device_name()
    print(f"GPU Device Name: {gpu_device_name}")
else:
    print("CUDA GPU is not available.")

def prediction_res(data_responses):
    window_size = WINDOW_SIZE
    converter_np_array = np.zeros((0, 3))

    len_BHE = len(data_responses["BHE"])
    len_BHN = len(data_responses["BHN"])
    len_BHZ = len(data_responses["BHZ"])

    min_len_channel = min(len_BHE,min(len_BHZ,len_BHN))

    data_start_time = data_responses['start_time']
    data_end_time = data_responses['end_time']
    
    for i in range(0,min_len_channel):
        BHE_channel = data_responses["BHE"][i]
        BHN_channel = data_responses["BHN"][i]
        BHZ_channel = data_responses["BHZ"][i]
        data_row = np.array([BHE_channel,BHN_channel,BHZ_channel])
        converter_np_array = np.vstack((converter_np_array, data_row))

    # STAT - START_TIME - END_TIME - Prediction
    np_array_with_prediction = np.zeros((0, 3))
    channel_time_stamp = np.array([data_responses['stat'],f"start-time : {data_responses['start_time']}", f"end-time : {data_responses['end_time']}"])

    # np_array_with_prediction = np.vstack((np_array_with_prediction, channel_time_stamp))
    
    utc_datetime_start_time = datetime.strptime(data_start_time, "%Y-%m-%dT%H:%M:%S.%fZ")
    utc_datetime_end_time = datetime.strptime(data_end_time, "%Y-%m-%dT%H:%M:%S.%fZ")

    total_prediction_a_minute = min_len_channel-window_size+1

    
    #start Precition time for 60 second
    start_predict_time = time.time()

    for i in range(0,min_len_channel-window_size+1):
        # blok np array yang akan berisi 120 baris data yang akan dimasukkan ke model ML
        prediction_array = np.zeros((0, 3))
        for j in range(i,i+window_size):
            # memasukkan data ke prediction array untuk data ke i sampai i + window_size
            # window_size => (20hz*range_waktu)
            BHE_channel = converter_np_array[j][0]
            BHN_channel = converter_np_array[j][1]
            BHZ_channel = converter_np_array[j][2]
            data_row = np.array([BHE_channel,BHN_channel,BHZ_channel])
            prediction_array = np.vstack((prediction_array, data_row))
        
    
        # normalisasi data
        normalize_data_array = normalize_data(prediction_array)
        # input ke model 

        normalize_data_array = normalize_data_array.reshape(1, WINDOW_SIZE,3)
        # predictions = model.predict(normalize_data_array,verbose=0)
        # predictions = 1.0

        # hasil prediksi
        prediction = random.random() 
        prediction_result = "No Earthquake."
        if(prediction > 0.6):
            prediction_result = "WARNING EARTHQUAKE !!"
            
        
        # if predictions[0][0] > 0.5:
        #     prediction_result = "WARNING EARTHQUAKE !!"
       
        # simpan kedalam database
        time_interval = timedelta(microseconds=i*50000)
        # block_prediction = np.array([f"<{data_responses['stat']}> Prediction Block : {i} time-stamp : {utc_datetime_start_time + time_interval}", f"Prediction Result : {prediction_result} !!"])
        prediction_timestamp = utc_datetime_start_time + time_interval
        prediction_timestamp_string = prediction_timestamp.isoformat()
        block_prediction = np.array([data_responses['stat'],prediction_timestamp_string,prediction_result])
        np_array_with_prediction = np.vstack((np_array_with_prediction, block_prediction))

    #end Precition time for 60 second
    end_predict_time = time.time()

    print(f"Result {data_responses['stat']}...")
    prdiction_process_time = end_predict_time - start_predict_time
    log_data(stat="PL",data=f'Prediction Length for 60 seconds : {total_prediction_a_minute}') 
    log_data(stat="PT",data=f'Prediction Time for 60 seconds : {prdiction_process_time}') 
    log_data(stat="DATA ID",data=f'ID : {data_responses["id"]}') 
    log_data(stat="TIME",data=f'start time : {data_responses["start_time"]}, end time : {data_responses["end_time"]}') 
    log_data(data=f'Shortest Channel length : {min_len_channel}', stat="Min Channel")
    return np_array_with_prediction

def normalize_data(data: np.ndarray):
    data = np.insert(np.diff(data, axis=0), 0, np.zeros((1, 3)), axis=0)
    return (data - np.mean(data, axis=0)) / np.std(data,axis=0)


if __name__ == "__main__":

    ref = db.reference("/prediction_all")
    ref_JAGI = db.reference("/prediction/JAGI")
    ref_SMRI = db.reference("/prediction/SMRI")
    ref_BBJI = db.reference("/prediction/BBJI")

    num_processes = 3
    consumer = Consumer(kafka_config)
    consumer.subscribe([kafka_topic])

    pool = multiprocessing.Pool(processes=num_processes)

    while True:
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
            results = pool.map(prediction_res, messages)

            # Handle results as needed
            # DICTIONARY FORMAT
            JAGI_Dic = []
            BBJI_Dic = []
            SMRI_Dic = []

            for index, predic_res in enumerate(results) :
                for data in predic_res:
                    if data[0] == "JAGI" :
                        # JAGI_Array = np.vstack((JAGI_Array, predic_res))
                        JAGI_Dic_temp = {}
                        JAGI_Dic_temp["station"] = data[0]
                        JAGI_Dic_temp["time_stamp"] = data[1]
                        JAGI_Dic_temp["prediction"] = data[2]
                        JAGI_Dic.append(JAGI_Dic_temp)
                    elif data[0] == "BBJI" :
                        # BBJI_Array = np.vstack((BBJI_Array, predic_res))
                        BBJI_Dic_temp = {}
                        BBJI_Dic_temp["station"] = data[0]
                        BBJI_Dic_temp["time_stamp"] = data[1]
                        BBJI_Dic_temp["prediction"] = data[2]
                        BBJI_Dic.append(BBJI_Dic_temp)
                    elif data[0] == "SMRI" :
                        # SMRI_Array = np.vstack((SMRI_Array, predic_res))
                        SMRI_Dic_temp = {}
                        SMRI_Dic_temp["station"] = data[0]
                        SMRI_Dic_temp["time_stamp"] = data[1]
                        SMRI_Dic_temp["prediction"] = data[2]
                        SMRI_Dic.append(SMRI_Dic_temp)

            # print(len(JAGI_Array))
            # print(JAGI_Dic)
            # # print(len(BBJI_Array))
            # print(BBJI_Dic)
            # # print(len(SMRI_Array))
            # print(SMRI_Dic)

            data_to_db = {
                'JAGI' : JAGI_Dic,
                'BBJI' : BBJI_Dic,
                'SMRI' : SMRI_Dic
            }

            # print(f"len : {len(results[0])}")
            # print(JAGI_Dic)

            try:
                # Use the push method to add data (creates a new unique key)
                new_record_ref = ref.push(data_to_db)
                new_record_ref_JAGI = ref_JAGI.push(JAGI_Dic)
                new_record_ref_SMRI = ref_SMRI.push(SMRI_Dic)
                new_record_ref_BBJI = ref_BBJI.push(BBJI_Dic)
                print("Data pushed to Database all successfully with key:", new_record_ref.key)
                print("Data pushed to Database JAGI successfully with key:", new_record_ref_JAGI.key)
                print("Data pushed to Database SMRI successfully with key:", new_record_ref_SMRI.key)
                print("Data pushed to Database BBJI successfully with key:", new_record_ref_BBJI.key)
            except Exception as e:
                print("Error pushing data:", e)