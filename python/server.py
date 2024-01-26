from flask import Flask
from confluent_kafka import Consumer, KafkaError
import os

app = Flask(__name__)

@app.route('/consume', methods=['POST'])
def consumeMessages():

  # Consumer configuration
  consumer_config = {
      'bootstrap.servers': 'pkc-4r087.us-west2.gcp.confluent.cloud:9092',  # e.g., 'localhost:9092'
      'group.id': 'python_example_group_1',           # Choose a consumer group ID
      'auto.offset.reset': 'earliest',                 # Start reading from the beginning of the topic if no offset is stored
      'security.protocol': 'SASL_SSL',
			'sasl.mechanisms': 'PLAIN',
			'sasl.username': os.environ('API_KEY'),
			'sasl.password': os.environ('API_SECRET'),
  }
  
  consumer = Consumer(consumer_config)

  topic = 'example'
  consumer.subscribe([topic])

  # Poll for new messages

  try:
    while True:
      msg = consumer.poll(1.0) #timeout in seconds
      if msg is None:
        continue
      if msg.error():
        if msg.error().code() == KafkaError._PARTITION_EOF:
          # End of partition event, not an error
          continue
        else:
          print(f"Error: {msg.error()}")
          break
      
      print(f"Received message: {msg.value().decode('utf-8')}")
  except KeyboardInterrupt:
    pass

  finally:
    #Close down cosumer to commit final offsets
    consumer.close()