from confluent_kafka import Producer, KafkaError, KafkaException
import certifi
from obspy.clients.fdsn import Client
from obspy import UTCDateTime
import time
from datetime import datetime
from obspy.clients.seedlink import Client as SeedlinkClient
from obspy.clients.seedlink.easyseedlink import create_client
import multiprocessing
import pytz
import json
import configparser

# fungsi callback error
def error_cb(err):
    print("Client error: {}".format(err))
    if err.code() == KafkaError._ALL_BROKERS_DOWN or \
       err.code() == KafkaError._AUTHENTICATION:
        raise KafkaException(err)

# fungsi untuk menampilkan log
def log_data(data, type="LOG",stat="-"):
    print(f"[+] <{stat}> <{type}>  {data}")

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

# anam topic pada confluent
kafka_topic = 'WAVE_Station' 
    
# menggunakan Producer dengan memasukkan kafka_config
producer = Producer(kafka_config)

def time_to_seconds(utc_date):
    hour = utc_date.hour
    minute = utc_date.minute
    second = utc_date.second
    total_seconds = hour * 3600 + minute * 60 + second
    return total_seconds

def acked(err, msg):
    """Delivery report callback called (from flush()) on successful or failed delivery of the message."""
    if err is not None:
        print('Failed to deliver message: {}'.format(err.str()))
    else:
        print('Produced to: {} [{}] @ {}'.format(msg.topic(), msg.partition(), msg.offset()))

def fetch_data(station_name):
    # Create a SeedLink client for the station
    client = SeedlinkClient("geofon.gfz-potsdam.de", 18000)
    
    while True:
        try:
            # Mengambil waktu saat ini
            utc_time = datetime.now(pytz.UTC)
            formatted_time = utc_time.strftime('%Y-%m-%dT%H:%M:%S')
            starttime = UTCDateTime(formatted_time) 
            endtime = starttime
            st = client.get_waveforms("GE", station_name, "*", "BH*", starttime - 60, endtime)
            value = {}

            for ch in st:
                value[ch.stats.channel] = ch.data.tolist()
                log_data(stat=station_name,data=f"Data from {ch.stats.channel} fetch succesfully !!")
                # interval detik data yang diambil
                time_interval = int(time_to_seconds(ch.stats.endtime)) - int(time_to_seconds(ch.stats.starttime))
                log_data(stat=station_name,data=f"from {ch.stats.starttime} to {ch.stats.endtime}")
                log_data(stat=station_name,data=f"Time interval of the collected data : {time_interval} seconds")
                log_data(stat=station_name,data=f'Waves Produced lenght : <{len(ch.data)}>')

            JSON_value = json.dumps(value)
            producer.produce(kafka_topic, key=station_name, value=JSON_value,callback=acked)
            log_data(stat=station_name,data=f'Waves Produced succesfully')

        except Exception as e:
            print(f"Error for station {station_name}: {str(e)}")
            time.sleep(2)


station_names = ["JAGI", "BBJI","SMRI"]
if __name__ == "__main__":
    with multiprocessing.Pool(processes=len(station_names)) as pool:
        pool.map(fetch_data, station_names)