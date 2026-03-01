from confluent_kafka import Producer, Consumer
from openai import OpenAI
import os
import json
import time
import sys

kafka_url = os.getenv('KAFKA_URL', 'localhost:9092')
topic_to_produce = os.getenv('TOPIC_TO_PRODUCE', 'output_topic')

print(f"PRODUCER AVVIATO | {kafka_url} | {topic_to_produce}")

producer = Producer({
    'bootstrap.servers': kafka_url
})


def delivery_report(err, msg):
    if err is not None:
        print(f"ERRORE DI CONSEGNA: {err}", flush=True, file=sys.stderr)
    else:
        print(
            f"CONSEGNATO AL TOPIC: {msg.topic()} [Partizione: {msg.partition()}]", flush=True)


def main():

    while True:
        data = {
            'domanda': 'Cosa si intende per Docker?'
        }
        print("Producendo il topic...", flush=True)
        producer.produce(
            topic=topic_to_produce,
            value=json.dumps(data).encode('utf-8'),
            callback=delivery_report
        )
        print("Topic prodotto.", flush=True)

        producer.poll(0)

        producer.flush()

        time.sleep(5)


if __name__ == '__main__':
    main()
