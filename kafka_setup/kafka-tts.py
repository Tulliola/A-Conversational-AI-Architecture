from confluent_kafka import Consumer, KafkaException
from openai import OpenAI
import os
import json
import logging
import traceback

tts_url = os.getenv('TTS_URL', "http://localhost:8880/v1")
shared_dir = os.getenv('SHARED_DIR', '/audio_files')

tts_client = OpenAI(api_key="none", base_url=tts_url)


# Kafka Context
kafka_url = os.getenv('KAFKA_URL', 'localhost:9092')
topic_to_consume = os.getenv('TOPIC_TO_CONSUME', 'llm_response')


def set_up_kafka_consumer():
    try:
        consumer = Consumer({
            "bootstrap.servers": kafka_url,
            "group.id": "tts-sidecar",
            "auto.offset.reset": "latest"
        })
        consumer.subscribe([topic_to_consume])
        tts_logger.info(f"Subscribed to {topic_to_consume} Kafka topic as a consumer!")

        return consumer
    except KafkaException as k:
        print(f"Raised exception during subscription: {k}")

# Logging context
tts_log_dir = os.getenv('TTS_LOGGING_ENDPOINT', '/data/logging/tts')

pod_id = os.getenv('MY_POD_ID', 'tts')

os.makedirs(f"{tts_log_dir}", exist_ok=True)

formatter = logging.Formatter(
    f'%(asctime)s [{pod_id}] %(levelname)s: %(message)s')


def setup_logger(logger_name, logfile_name):
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)

    handler = logging.FileHandler(logfile_name)
    handler.setFormatter(formatter)

    logger.addHandler(handler)

    return logger


tts_logger = setup_logger("TTS_Logger", f"{tts_log_dir}/{pod_id}.log")


def main():
    consumer = set_up_kafka_consumer()

    while True:
        try:
            message = consumer.poll(timeout=1.0)

            if message and message.error():
                tts_logger.error(f"Consumer error: {message.error()}.")
            elif message:
                message = json.loads(message.value().decode('utf-8'))
                llm_response = message['text']
                conversation_id = message['conv_id']

                tts_logger.info(f"[CONV_ID: {conversation_id}] Starting audio synthesis...")
                with tts_client.audio.speech.with_streaming_response.create(
                    model="kokoro",
                    input=llm_response,
                    voice="af_bella"
                ) as response:
                    response.stream_to_file(os.path.join(
                        shared_dir, "answer_test.mp3"))
                tts_logger.info(f"[CONV_ID: {conversation_id}] Audio synthesis completed!")
        except Exception as e:
            tts_logger.error(f"Error occured: {e}")
            tts_logger.error(traceback.format_exc)


if __name__ == '__main__':
    main()
