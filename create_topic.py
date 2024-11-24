from kafka.admin import KafkaAdminClient, NewTopic
from configs import kafka_config

# Create Kafka Admin Client
admin_client = KafkaAdminClient(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password']
)

# Define topics to create
my_name = "romanchuk"  # Append to topic names for uniqueness
topics = [
    NewTopic(name=f'building_sensors_{my_name}', num_partitions=2, replication_factor=1),
    NewTopic(name=f'temperature_alerts_{my_name}', num_partitions=2, replication_factor=1),
    NewTopic(name=f'humidity_alerts_{my_name}', num_partitions=2, replication_factor=1),
]

# Create topics
try:
    admin_client.create_topics(new_topics=topics, validate_only=False)
    print(f"Topics created successfully: {[topic.name for topic in topics]}")
except Exception as e:
    if "TopicExistsException" in str(e):
        print("One or more topics already exist.")
    else:
        print(f"An error occurred: {e}")

# List existing topics
print("Available topics:", admin_client.list_topics())

# Close Kafka Admin Client
admin_client.close()
