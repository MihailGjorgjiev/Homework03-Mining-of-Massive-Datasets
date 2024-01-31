import json

import joblib
import pandas as pd
from kafka import KafkaConsumer, KafkaProducer

if __name__ == '__main__':
    bootstrap_servers = 'localhost:9092'
    input_topic = 'health_data'
    output_topic = 'health_data_predicted'

    producer = KafkaProducer(bootstrap_servers='localhost:9092', security_protocol="PLAINTEXT", api_version=(2, 0, 2))
    consumer = KafkaConsumer(input_topic,
                             bootstrap_servers=bootstrap_servers,
                             group_id='my_consumer_group',
                             auto_offset_reset='latest',  # Start reading from the beginning if no offset is stored
                             enable_auto_commit=False,
                             api_version=(2, 0, 2))  # Disable auto-commit to manually control offsets
    model=joblib.load("diabetes_prediction_model.pkl")
    try:
        for message in consumer:
            record=message.value.decode('utf-8')
            record=json.loads(record)
            cols=record['columns']
            vals=record['value']
            data = {col: [val] for col, val in zip(cols, vals)}
            sample=pd.DataFrame(data)
            prediction = model.predict(sample)[0]

            record['columns'].append("prediction")
            record['value'].append(float(prediction))

            producer.send(
                topic=output_topic,
                value=json.dumps(record).encode("utf-8")
            )


    except KeyboardInterrupt:
        pass
    finally:
        # Close the consumer
        consumer.close()




