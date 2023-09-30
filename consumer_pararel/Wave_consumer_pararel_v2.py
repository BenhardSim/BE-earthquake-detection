from confluent_kafka import Consumer, KafkaError
import tensorflow as tf
from tensorflow import keras
import multiprocessing
import certifi
import uuid
import configparser
import datetime
import json
import numpy as np
import logging

unique_group_id = f'my-consumer-group-{str(uuid.uuid4())}'

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
model = keras.models.load_model('../tensorflow_model/conv1d_lstm_window4.0s_20hz_diff_then_scale_window_diff_and_scaling_v2.h5')

# connect ke confluen kafka
consumer = Consumer(kafka_config)

# mensbscribe kafka topic 
kafka_topic = 'wave_station_v3' 
consumer.subscribe([kafka_topic])

def log_data(data, type="LOG",stat="-"):
    print(f"[+] <{stat}> <{type}>  {data}")

def prediction_res(data_responses):
    window_size = WINDOW_SIZE
    converter_np_array = np.zeros((0, 3))

    len_BHE = len(data_responses["BHE"])
    len_BHN = len(data_responses["BHN"])
    len_BHZ = len(data_responses["BHZ"])

    min_len_channel = min(len_BHE,min(len_BHZ,len_BHN))

    for i in range(0,min_len_channel):
        BHE_channel = data_responses["BHE"][i]
        BHN_channel = data_responses["BHN"][i]
        BHZ_channel = data_responses["BHZ"][i]
        data_row = np.array([BHE_channel,BHN_channel,BHZ_channel])
        converter_np_array = np.vstack((converter_np_array, data_row))

    np_array_with_prediction = np.zeros((0, 2))

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
        # print(normalize_data_array)
        # input ke model 

        normalize_data_array = normalize_data_array.reshape(1, WINDOW_SIZE,3)
        # predictions = model.predict(normalize_data_array,verbose=0)
        # predictions = 1.0

        # hasil prediksi
        prediction_result = "No Earthquake."
        # if predictions[0][0] > 0.5:
        #     prediction_result = "WARNING EARTHQUAKE !!"
        
        # log hasil prediksi 
        # log_data(data=f'Prediction for Block {i} : {prediction_result}', stat=f"Result {stat.decode('utf-8')}")

        # simpan kedalam database
        block_prediction = np.array([f"Prediction Block : {i}", f"Prediction Result : {prediction_result} !!"])
        np_array_with_prediction = np.vstack((np_array_with_prediction, block_prediction))


    print("Result...")
    log_data(stat="DATA ID",data=f'ID : {data_responses["id"]}') 
    log_data(stat="TIME",data=f'start time : {data_responses["start_time"]}, end time : {data_responses["end_time"]}') 
    log_data(data=f'Shortest Channel length : {min_len_channel}', stat="Min Channel")
    return np_array_with_prediction

def normalize_data(data: np.ndarray):
    data = np.insert(np.diff(data, axis=0), 0, np.zeros((1, 3)), axis=0)
    return (data - np.mean(data, axis=0)) / np.std(data,axis=0)


if __name__ == "__main__":
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
            print(results)