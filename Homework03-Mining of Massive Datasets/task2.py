from kafka import KafkaProducer
import pandas as pd
import json
import time
import random
# import numpy as np
# Kafka configuration
bootstrap_servers = 'localhost:9092'
topic = 'health_data'

df=pd.read_csv("./online.csv").iloc[:,1:].to_json(orient="split")
df=json.loads(df)


producer = KafkaProducer(bootstrap_servers='localhost:9092', security_protocol="PLAINTEXT",api_version=(2,0,2))
for i in range(len(df['data'])):
	key=df['index'][i]
	columns=df['columns']
	value=df['data'][i]
	record = {
		'key': key,
		'columns':columns,
		'value': value,
		'timestamp': int(time.time() * 1000)
	}
	print(json.dumps(record))

	producer.send(
		topic=topic,
		value=json.dumps(record).encode("utf-8")
	)
	time.sleep(random.randint(500, 2000) / 1000.0)
# Create a Kafka consumer
