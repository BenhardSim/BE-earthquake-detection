from confluent_kafka import Consumer, KafkaError
import certifi
import uuid
import configparser

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
    'auto.offset.reset': 'earliest'
}


# connect ke confluen kafka
consumer = Consumer(kafka_config)

# mensbscribe kafka topic 
kafka_topic = 'WAVE_Station' 
consumer.subscribe([kafka_topic])

while True:
    msg = consumer.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        if msg.error().code() == KafkaError._PARTITION_EOF:
            print(f'Reached end of partition for topic {msg.topic()} [{msg.partition()}]')
        else:
            print(f'Error while consuming message: {msg.error()}')
    else:
        # Process the received message
        partition_key = msg.key()
        print(f'Message Key: {partition_key.decode("utf-8")}')
        print(f'Received message: {msg.value().decode("utf-8")}')
        # print(f'Received message length: {len(msg[partition_key]["BHE"])}')
        # input ke model ML
        
