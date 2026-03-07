from confluent_kafka import Consumer, KafkaException
from openai import OpenAI
import os
import json

kafka_url = os.getenv('KAFKA_URL', 'localhost:9092')
topic_to_consume = os.getenv('TOPIC_TO_CONSUME', 'llm_response')
tts_url = os.getenv('TTS_URL', "http://localhost:8880/v1")
shared_dir = os.getenv('SHARED_DIR', '/audio_files')

tts_client = OpenAI(api_key="none", base_url=tts_url)

def set_up_kafka_consumer():
    try:
        consumer = Consumer({
            "bootstrap.servers": kafka_url,
            "group.id": "tts-sidecar",
            "auto.offset.reset": "latest"
        })
        consumer.subscribe([topic_to_consume])
        print(f"Subscribed to topic {topic_to_consume}", flush=True)

        return consumer
    except KafkaException as k:
        print(f"Raised exception during subscription: {k}")

consumer = set_up_kafka_consumer()

def main():
    while True:
        try:
            message = consumer.poll(timeout=1.0)

            if message and message.error():
                print(f"Consumer error: {message}", flush=True)
            elif message:
                message = json.loads(message.value().decode('utf-8'))
                llm_response = message['text']

                with tts_client.audio.speech.with_streaming_response.create(
                    model="kokoro",
                    input=llm_response,
                    voice="af_bella"
                ) as response:
                    response.stream_to_file(os.path.join(shared_dir, "answer_test.mp3"))
        except Exception as e:
            print(f"Error occured during consumer polling: {e}")


if __name__ == '__main__':
    main()
