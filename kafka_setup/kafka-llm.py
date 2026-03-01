from confluent_kafka import Producer, Consumer
import os
import json
import sys

kafka_url = os.getenv('KAFKA_URL', 'localhost:9092')
topic_to_consume = os.getenv('TOPIC_TO_CONSUME', 'input_topic')
topic_to_produce = os.getenv('TOPIC_TO_PRODUCE', 'output_topic')

print(f"CONSUMER AVVIATO | {kafka_url} | {topic_to_consume} | {topic_to_produce}")

consumer = Consumer({
    'bootstrap.servers': kafka_url,
    'group.id': 'sidecar-kafka',
    'auto.offset.reset': 'earliest'
})

def process_received_msg(msg):
    value = msg.value().decode('utf-8')
    json_msg = json.loads(value)
    print(f"Received message: {json_msg}", flush= True)

    # Occupati di inviare l'evento lato producer


def main():
    try:
        consumer.subscribe([topic_to_consume])
        print(f"Subscribed to topic {topic_to_consume}", flush= True)

        while True:
            msg = consumer.poll(timeout=1.0)
            
            if msg and msg.error():
                print(f"Consumer error: {msg}", flush= True)
            elif msg:
                process_received_msg(msg)
    finally:
        consumer.close()


if __name__ == '__main__':
    main()
