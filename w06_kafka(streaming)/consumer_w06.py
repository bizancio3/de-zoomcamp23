import json
from pykafka import KafkaClient

client = KafkaClient(hosts="localhost:9092")
topic = client.topics[b'my_topic']

consumer = topic.get_simple_consumer()

counts = {}  # Dictionary to store PUlocationID counts

for message in consumer:
    if message is not None:
        # Decode message value from bytes to string
        value = message.value().decode('utf-8')
        data = json.loads(value)
        
        # Extract PUlocationID and value from message
        PUlocationID = data['PUlocationID']
        value = data['value']
        
        # Update counts dictionary
        if PUlocationID in counts:
            counts[PUlocationID] += value
        else:
            counts[PUlocationID] = value
        
        # Print intermediate counts for each batch
        print(counts)
    
# Aggregate counts across all batches
total_counts = {}
for count in counts.values():
    for PUlocationID, value in count.items():
        if PUlocationID in total_counts:
            total_counts[PUlocationID] += value
        else:
            total_counts[PUlocationID] = value

# Print final counts
print(total_counts)
