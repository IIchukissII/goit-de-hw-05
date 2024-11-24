import time
from datetime import datetime
from kafka import KafkaConsumer, KafkaProducer
from configs import kafka_config
import json

# Kafka Consumer for building_sensors
consumer = KafkaConsumer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    key_deserializer=lambda v: json.loads(v.decode('utf-8')) if v else None,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my_consumer_group_1'
)

# Kafka Producers for alert topics
producer = KafkaProducer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Define topics
input_topic = "building_sensors_romanchuk"
temperature_alert_topic = "temperature_alerts_romanchuk"
humidity_alert_topic = "humidity_alerts_romanchuk"

consumer.subscribe([input_topic])

print(f"Subscribed to topic '{input_topic}'")

try:
    for message in consumer:
        # Parse message
        data = message.value
        print(f"Received: {data}")

        sensor_id = data.get("sensor_id")
        timestamp = data.get("timestamp")
        temperature = data.get("temperature")
        humidity = data.get("humidity")
        # Check for alerts
        if temperature > 40:
            alert = {
                "sensor_id": sensor_id,
                "timestamp": timestamp,
                "temperature": temperature,
                "message": "High temperature detected!"
            }
            print(f"Temperature Alert: {alert}")
            producer.send(temperature_alert_topic, alert)

        if humidity < 20 or humidity > 80:
            alert = {
                "sensor_id": sensor_id,
                "timestamp": timestamp,
                "humidity": humidity,
                "message": "Humidity out of bounds!"
            }
            print(f"Humidity Alert: {alert}")
            producer.send(humidity_alert_topic, alert)

        # Simulate processing delay
        time.sleep(0.5)

except Exception as e:
    print(f"An error occurred: {e}")
finally:
    consumer.close()
    producer.close()
