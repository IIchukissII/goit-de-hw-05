from kafka import KafkaConsumer
from configs import kafka_config
import json

RED = '\033[91m'
RESET = '\033[0m'

# Kafka Consumer for both temperature and humidity alerts
alerts_consumer = KafkaConsumer(
    'temperature_alerts_romanchuk',
    'humidity_alerts_romanchuk',
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='alerts_group'
)

print("Listening for alerts...")

try:
    for message in alerts_consumer:
        topic = message.topic
        data = message.value

        if topic == 'temperature_alerts_romanchuk':
            print(f"{RED}Temperature Alert: {data['message']}{RESET}")
        elif topic == 'humidity_alerts_romanchuk':
            print(f"{RED}Humidity Alert: {data['message']}{RESET}")

except Exception as e:
    print(f"An error occurred: {e}")
finally:
    alerts_consumer.close()
