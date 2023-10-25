# Import necessary libraries and modules.
import kafka
import time
import random
import json
from time import sleep

# Define a dictionary to hold sensor data for longitude and latitude.
sensor_data = {'longitude': 0, 'latitude': 0}

# Define the name of the Kafka topic to be used.
topic_name = "vehicle-coordinates"

# Establish a connection to the Kafka cluster using the specified broker address.
client = kafka.KafkaClient(bootstrap_servers=['localhost:9092'])

# Create a Kafka producer instance with the specified broker address and a serializer to convert data to JSON format.
producer = kafka.KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: 
                         json.dumps(x).encode('utf-8'))

# Create a Kafka consumer instance with the specified broker address.
consumer = kafka.KafkaConsumer(bootstrap_servers=['localhost:9092'])

# Define a callback function to handle acknowledgment after sending a message to Kafka.
def acked(err, msg):
    if err is not None:
        print("Failed to deliver message: {0}: {1}"
              .format(msg.value(), err.str()))
    else:
        print("Message produced: {0}".format(msg.value()))

try:
    # Check if the specified topic already exists.
    if topic_name in consumer.topics():
         print(topic_name+" exist")
    else:
        # Ensure the topic exists.
        client.ensure_topic_exists(topic_name)

    # Close the consumer and client connections.
    consumer.close()
    client.close()

    # Continuously generate random longitude and latitude values and send them to the Kafka topic.
    while True:
        longitude = random.randint(-180, 180)
        latitude = random.randint(-90, 90)
        
        print(f"longitude: {longitude} latitude: {latitude}")
        sensor_data['longitude'] = longitude
        sensor_data['latitude'] = latitude

        # Send the updated sensor_data to the Kafka topic.
        producer.send(topic_name, value=sensor_data)

        # Wait for 3 seconds before generating the next set of values.
        sleep(3)

# Allow the program to be interrupted using keyboard input (e.g., Ctrl+C).
except KeyboardInterrupt:
    pass
