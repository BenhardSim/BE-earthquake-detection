from confluent_kafka import Producer, KafkaError, KafkaException
import certifi
from obspy.clients.fdsn import Client
from obspy import UTCDateTime
import schedule
import time
from datetime import datetime
from obspy.clients.seedlink import Client as SeedlinkClient
from obspy.clients.seedlink.easyseedlink import create_client
import pytz

def error_cb(err):
    print("Client error: {}".format(err))
    if err.code() == KafkaError._ALL_BROKERS_DOWN or \
       err.code() == KafkaError._AUTHENTICATION:
        # Any exception raised from this callback will be re-raised from the
        # triggering flush() or poll() call.
        raise KafkaException(err)

kafka_config = {
    'bootstrap.servers': "pkc-921jm.us-east-2.aws.confluent.cloud:9092", 
    'sasl.mechanism': 'PLAIN',
    'security.protocol': 'SASL_SSL',
    'sasl.username': 'OECIXLLKNT7H4IDK',
    'sasl.password': 'MitUuO9LNEf3PXAaHlzrsFot3h8b8Ez36RhFRC5RL8B4zA3sGV/skq/IRHot8az8',
    "ssl.ca.location": certifi.where(),
    'linger.ms': 100,
    'error_cb': error_cb, 
}

#topic
kafka_topic = 'JAGI_wave' 

#producer
producer = Producer(kafka_config)
client = SeedlinkClient("geofon.gfz-potsdam.de", 18000)

def log_data(data, type="LOG"):
    print(f"[+] <{type}> {data}")

def get_data_seed_link():
    # Get the current time in UTC
    utc_time = datetime.now(pytz.UTC)
    formatted_time = utc_time.strftime('%Y-%m-%dT%H:%M:%S')
    starttime = UTCDateTime(formatted_time)
    endtime = starttime

    st = client.get_waveforms("GE", "JAGI", "*", "BH*", starttime - 60*2, endtime - 60)
    value = ""
    for ch in st:
        value = value + str(ch.data)
        # log_data(f"Received {ch.stats.npts} data(s)")
        # log_data(f"from {ch.stats.starttime} to {ch.stats.endtime}")
        # log_data(f"Delta: {ch.stats.delta}")

    return st

def acked(err, msg):
    """Delivery report callback called (from flush()) on successful or failed delivery of the message."""
    if err is not None:
        print('Failed to deliver message: {}'.format(err.str()))
    else:
        print('Produced to: {} [{}] @ {}'.format(msg.topic(), msg.partition(), msg.offset()))

keyval = 1000
while True:
    try:
        values = get_data_seed_link()
        for value in values:
            # print(value.data, "\n")
            producer.produce(kafka_topic, key=str(value.stats.channel), value=str(value.data),callback=acked)
            print(f'Produced message wave size: {len(value.data)}')
        # schedule.run_pending()
        # time.sleep(1)
        # producer.produce(kafka_topic, key=str(keyval), value="test speed data",callback=acked)
        # print("testing push {keyval} \n")
        # keyval += 1
    except:
        print("No Trace Data\n")
        time.sleep(1)

# try:
#     # Produce test messages
#     # for i in range(10):  # Produce 10 test messages
#     #     message = f'Test Message niceee broo {i+10}'
#     #     producer.produce(kafka_topic, key=str(i+10), value=message,callback=acked)
#     #     print(f'Produced message: {message}')

#     # # Wait for any outstanding messages to be delivered and delivery reports received
#     # producer.flush()
#     while True:
#         get_data_seed_link()
#         # schedule.run_pending()
#         # time.sleep(1)
#     # except:
#     #     print("No Trace Data\n")
#     #     time.sleep(1)

# except Exception as e:
#     print(f"Message delivery failed: {str(e)}")
