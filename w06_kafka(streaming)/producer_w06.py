import csv
import json
from pykafka import KafkaClient

client = KafkaClient(hosts="localhost:9092")
topic = client.topics[b'my_topic']

# Read green trip data and publish to Kafka topic
with open('green_tripdata_2019-01.csv.gz', 'r') as file:
    reader = csv.DictReader(file)
    for row in reader:
        data = {'PUlocationID': row['PULocationID'], 'value': 1}
        message = json.dumps(data).encode('utf-8')
        with topic.get_sync_producer() as producer:
            producer.produce(message)

# Read fhv trip data and publish to Kafka topic
with open('fhv_tripdata_2019-01.csv.gz', 'r') as file:
    reader = csv.DictReader(file)
    for row in reader:
        data = {'PUlocationID': row['PULocationID'], 'value': 1}
        message = json.dumps(data).encode('utf-8')
        with topic.get_sync_producer() as producer:
            producer.produce(message)
