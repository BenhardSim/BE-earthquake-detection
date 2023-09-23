from confluent_kafka import Producer, KafkaError, KafkaException
import certifi
from obspy.clients.fdsn import Client
from obspy import UTCDateTime
from datetime import datetime
from obspy.clients.seedlink import Client as SeedlinkClient
from obspy.clients.seedlink.easyseedlink import create_client
import pytz
import json
import configparser
import threading
import signal

# fungsi callback error
def error_cb(err):
    print("Client error: {}".format(err))
    if err.code() == KafkaError._ALL_BROKERS_DOWN or \
       err.code() == KafkaError._AUTHENTICATION:
        raise KafkaException(err)
    
# Create a ConfigParser instance
config = configparser.ConfigParser()
# Load the configuration from the file
config.read('kafka_config.txt')

bootstrap_servers = config.get('KafkaConfig', 'bootstrap.servers')
sasl_username = config.get('KafkaConfig', 'sasl.username')
sasl_password = config.get('KafkaConfig', 'sasl.password')
    
# config untuk masuk ke akun confluent 
kafka_config = {
    'bootstrap.servers': bootstrap_servers, 
    'sasl.mechanism': 'PLAIN',
    'security.protocol': 'SASL_SSL',
    'sasl.username': sasl_username,
    'sasl.password': sasl_password,
    "ssl.ca.location": certifi.where(),
    'linger.ms': 100,
    'error_cb': error_cb, 
}

# nama topic pada confluent
kafka_topic = 'wave_station_v2' 
# menggunakan Producer dengan memasukkan kafka_config
producer = Producer(kafka_config)
# seedlink untuk mengambil data dari stasiun sensor dengan realtime
client = SeedlinkClient("geofon.gfz-potsdam.de", 18000)

# fungsi untuk menampilkan log
def log_data(data, type="LOG"):
    print(f"[+] <{type}> {data}")


# Mengambil data dengan SeedLink Client
def get_data_seed_link(key_partition):
    while True:
        try:
            # Mengambil waktu saat ini
            utc_time = datetime.now(pytz.UTC)
            formatted_time = utc_time.strftime('%Y-%m-%dT%H:%M:%S')
            starttime = UTCDateTime(formatted_time) 
            endtime = starttime
            print("Producing data...")
            st = client.get_waveforms("GE", key_partition, "*", "BH*", starttime - 60*2, endtime - 60)
            
            # Extract and log the data
            value = {}
            log_data(f"Data from {key_partition} station")
            for ch in st:
                value[ch.stats.channel] = ch.data.tolist()
                log_data(f"Data from {ch.stats.channel} fetch successfully !!")
                log_data(f'Waves Produced length: <{len(ch.data)}>')

            # Mengubah file dalam bentuk JSON
            JSON_value = json.dumps(value)
            
            # Produce the JSON data to Kafka for the channel
            producer.produce(kafka_topic, key=key_partition.encode('utf-8'), value=JSON_value.encode('utf-8'))
            producer.flush()

            print("\n")

        except Exception as e:
            log_data(f"Error fetching or producing data for key_partition {key_partition}: {str(e)}")

station_java = ['JAGI', 'SMRI'] 

# Create threads to fetch and produce data for each key_partition
threads = []
exit_signal = threading.Event()
for station in station_java:
    thread = threading.Thread(target=get_data_seed_link, args=(station,))
    threads.append(thread)
    thread.start()

# Function to gracefully stop the threads and close the producer
def stop_threads(signal, frame):
    print("Stopping threads...")
    exit_signal.set()  # Set the exit signal to stop the threads

# Set the signal handler for SIGINT (Ctrl+C)
signal.signal(signal.SIGINT, stop_threads)

# Wait for threads to finish
for thread in threads:
    thread.join()

# Close the Kafka producer
producer.close()

