from confluent_kafka import Producer, Consumer
import os
import json
import time
import traceback
import logging
import sys

# Logging context
asr_log_dir = os.getenv('ASR_LOGGING_ENDPOINT', '/data/logging/asr')

pod_id = os.getenv('MY_POD_ID', 'asr')

# os.makedirs(f"{asr_log_dir}", exist_ok=True)

formatter = logging.Formatter(
    f'%(asctime)s [%(name)s] [{pod_id}] %(levelname)s: %(message)s')


def setup_logger(logger_name):
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)

    logger.addHandler(handler)

    return logger


asr_logger = setup_logger("ASR_Logger")

# Kafka context
kafka_url = os.getenv('KAFKA_URL', 'localhost:9092')
topic_to_produce = os.getenv('TOPIC_TO_PRODUCE', 'user_query')

asr_logger.info(
    f"ASR Producer ready to publish on {topic_to_produce} Kafka topic ({kafka_url})!")

producer = Producer({
    'bootstrap.servers': kafka_url
})


def delivery_report(err, msg, conv_id):
    if err is not None:
        asr_logger.error(
            f"[CONV_ID: {conv_id}] Error occured during publishing: {err}")
    else:
        asr_logger.info(
            f"[CONV_ID: {conv_id}] Message published on {msg.topic()} Kafka topic!")


def main():

    while True:
        try:
            conversation_id = 1
            message = {
                "text": "Which holiday occurs on 1st January?",
                "conv_id": conversation_id
            }
            asr_logger.info(
                f"[CONV_ID: {conversation_id}] Publishing message on {topic_to_produce} Kafka topic...")
            producer.produce(
                topic=topic_to_produce,
                value=json.dumps(message).encode('utf-8'),
                callback=lambda err, msg: delivery_report(
                    err, msg, conversation_id)
            )

            producer.poll(0)

            producer.flush()

            time.sleep(60)
        except Exception as e:
            producer.flush()
            asr_logger.error(f"Error occured: {e}")
            asr_logger.error(traceback.format_exc())


if __name__ == '__main__':
    main()
