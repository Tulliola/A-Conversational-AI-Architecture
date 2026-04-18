from typing import TypedDict, Annotated, Sequence
from langchain_core.tools import tool
from langchain_openai import ChatOpenAI
from langchain_core.messages import BaseMessage, SystemMessage
from langgraph.graph.message import add_messages
from langgraph.prebuilt import ToolNode, InjectedState
from langgraph.graph import StateGraph, START, END
from confluent_kafka import Consumer, Producer, KafkaException, KafkaError
from neo4j import GraphDatabase
from openai import OpenAI
import os
import json
import sys
import traceback
import logging

# Logging context
llm_log_dir = os.getenv('LLM_LOGGING_ENDPOINT', '/data/logging/llm')
emb_log_dir = os.getenv('EMB_LOGGING_ENDPOINT', '/data/logging/embedding')
dm_log_dir = os.getenv('DM_LOGGING_ENDPOINT', '/data/logging/dialog_manager')

pod_id = os.getenv('MY_POD_ID', 'dialog-manager')

os.makedirs(f"{llm_log_dir}", exist_ok=True)
os.makedirs(f"{emb_log_dir}", exist_ok=True)
os.makedirs(f"{dm_log_dir}", exist_ok=True)

formatter = logging.Formatter(
    f'%(asctime)s [{pod_id}] %(levelname)s: %(message)s')


def setup_logger(logger_name, logfile_name):
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)

    handler = logging.FileHandler(logfile_name)
    handler.setFormatter(formatter)

    logger.addHandler(handler)

    return logger


llm_logger = setup_logger(
    "LLM_Logger", f"{llm_log_dir}/llm_calls-{pod_id}.log")
emb_logger = setup_logger(
    "EMB_Logger", f"{emb_log_dir}/emb_calls-{pod_id}.log")

# Kafka context
kafka_url = os.getenv('KAFKA_URL', 'localhost:9092')
topic_to_consume = os.getenv('TOPIC_TO_CONSUME', 'conversation_history')
topic_to_produce = os.getenv('TOPIC_TO_PRODUCE', 'llm_response')


def set_up_kafka_consumer():
    try:
        consumer = Consumer({
            'bootstrap.servers': kafka_url,
            'group.id': 'dialog-manager-group',
            'auto.offset.reset': 'latest',
            'enable.auto.commit': False
        })
        consumer.subscribe([topic_to_consume])

        return consumer
    except KafkaException as k:
        sys.exit(1)


def set_up_kafka_producer():
    return Producer({
        'bootstrap.servers': kafka_url
    })

def delivery_report(err, msg, conv_id, logger):
    if err is not None:
        logger.error(f"[CONV_ID: {conv_id}] Error occured during publishing: {err}")
    else:
        logger.info(f"[CONV_ID: {conv_id}] Message published on {msg.topic()} Kafka topic!")


# Neo4j variables
db_uri = os.getenv('DB_URL', 'neo4j://db-headless-service:7687')
db_auth = (os.getenv('DB_AUTH_USER', 'neo4j'),
           os.getenv('DB_AUTH_PASSWORD', 'password'))
neo4j_driver = GraphDatabase.driver(db_uri, auth=db_auth)

# LLM variables
model_name = os.getenv('MODEL_NAME', "Qwen/Qwen3-4B-Instruct-2507")
model_url = os.getenv('LLM_URL', "http://llm-service:80/v1")

# Embedding variables
embedding_name = os.getenv('EMBEDDING_MODEL_NAME',
                           "text-embedding-embeddinggemma-300m")
embedding_url = os.getenv('EMBEDDING_URL', "http://embedding-service:80/v1")

embedding_model = OpenAI(
    api_key="none",
    base_url=embedding_url
)


def get_embedding(text, conversation_id):
    emb_logger.info(f"[CONV_ID: {conversation_id}] Embedding model invoked...")
    embedding = embedding_model.embeddings.create(
        input=[text.replace('\n', ' ')],
        model=embedding_name).data[0].embedding
    emb_logger.info(f"[CONV_ID: {conversation_id}] Embedding produced!")
    return embedding


class AgentState(TypedDict):
    messages: Annotated[Sequence[BaseMessage], add_messages]
    conversation_id: int


@tool
def retrieval_augmented_generation(user_query: str, state: Annotated[dict, InjectedState]) -> list[str]:
    """
    Usa questo tool SOLO SE l'utente fa esplicitamente riferimento alla patologia dell'epilessia
    o il contesto della conversazione si riferisce CHIARAMENTE a questa pataologia. In questi casi
    cerca nel database vettoriale informazioni cliniche e mediche sull'epilessia.


    NON usare questo tool se l'utente fa domande molto vaghe, come "A che età compaiono i primi sintomi?" 
    o "Come la si diagnostica?" e non hai modo di capire che l'utente fa effettivamente riferimento all'epilessia. In questi casi
    chiedi chiarimenti all'utente.

    Args:
        user_query: La domanda esatta o il concetto sull'epilessia cercato dall'utente.
    """

    llm_logger.debug(
        f"[CONV_ID: {state['conversation_id']}] LLM model has requested RAG tool.")

    user_query_embedding = get_embedding(user_query, state['conversation_id'])

    cypher_query = """
        CALL db.index.vector.queryNodes("embeddingIndex", 3, $embedding)
        YIELD node, score
        WITH node AS emb, score AS affinity ORDER BY affinity DESC
        MATCH (c:CHUNK)-[:HAS_QUESTION|HAS_STATEMENT]-(k:QUESTION|STATEMENT)-[:HAS_EMBEDDING]-(emb:EMBEDDING)
        RETURN DISTINCT c.text AS text
    """

    records, _, _ = neo4j_driver.execute_query(
        cypher_query,
        embedding=user_query_embedding,
        database_="neo4j"
    )
    return [(record["text"]) for record in records]


@tool
def holiday_calendar(date: str, state: Annotated[dict, InjectedState]) -> str:
    """
    Questo tool DEVE essere invocato ogni qualvolta l'utente fa riferimento ad una data e vuole conoscere la festività che cade in quella data.
    Restituisce la festività che cade in quella data specificata dall'utente.

    Args:
        date: la data a cui l'utente fa riferimento.
    """

    llm_logger.debug(
        f"[CONV_ID: {state['conversation_id']}] LLM model has requested holiday_calendar tool.")

    return "Natale"


def model_call(state: AgentState) -> AgentState:
    llm_logger.info(
        f"[CONV_ID: {state['conversation_id']}] LLM model invoked...")
    response = model.invoke(state['messages'])
    llm_logger.info(
        f"[CONV_ID: {state['conversation_id']}] LLM model response generation ended!")

    return {'messages': [response], 'conversation_id': state['conversation_id']}


def should_continue(state: AgentState):
    last_message = state['messages'][-1]

    return hasattr(last_message, 'tool_calls') and len(last_message.tool_calls) > 0


tools = [holiday_calendar, retrieval_augmented_generation]
stop_tokens = ["<|im_end|>", "<|im_start|>", "<|endoftext|>"]

model = ChatOpenAI(
    model=model_name,
    api_key="NONE",
    base_url=model_url,
    stop_sequences=stop_tokens
).bind_tools(tools)


graph = StateGraph(AgentState)
graph.add_node("model_call", model_call)
graph.add_node("tools", ToolNode(tools=tools))

graph.set_entry_point("model_call")
graph.add_conditional_edges(
    "model_call",
    should_continue,
    {
        True: "tools",
        False: END
    }
)
graph.add_edge("tools", "model_call")

app = graph.compile()


def main():
    dm_logger = setup_logger("DIALOG_MANAGER_Logger",
                             f"{dm_log_dir}/{pod_id}.log")

    dm_logger.info("Starting Dialog Managing...")

    consumer = set_up_kafka_consumer()
    dm_logger.info(f"Subscribed to {topic_to_consume} Kafka topic as a consumer!")

    producer = set_up_kafka_producer()
    while True:
        try:
            message = consumer.poll(timeout=1.0)

            if message is None:
                continue

            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    dm_logger.error(
                        f"Reached end of partition {message.topic()} [{message.partition()}].")
                else:
                    dm_logger.error(f"Consumer error: {message.error()}.")
            else:
                payload = json.loads(message.value().decode('utf-8'))

                if 'text' not in payload and 'conv_id' not in payload:
                    dm_logger.error(f"Received malformed message {payload}.")
                    continue

                conversation_id = payload['conv_id']
                conversation_history = [(
                    "system",
                    "Sei un assistente olografico personale specializzato. "
                    "Rispondi alle domande al meglio delle tue capacità. "
                    "- Se l'utente fa esplicitamente domande specifiche sull'epilessia o riesci a capire dal contesto che sta facendo riferimento"
                    "all'epilessia DEVI assolutamente utilizzare il tool 'retrieval_augmented_generation' per cercare le informazioni prima di rispondere. "
                    "Altrimenti, chiedi all'utente di essere meno ambiguo."
                    "- Se l'utente chiede informazioni sulle date, usa il tool 'holiday_calendar'.")]
                conversation_history = conversation_history + \
                    [(message['role'], message['text'])
                     for message in payload['history']]

                dm_logger.info(
                    f"[CONV_ID: {conversation_id}] Received message from {topic_to_consume} Kafka topic with content: {payload}.")
                dm_logger.info(
                    f"[CONV_ID: {conversation_id}] Starting Dialog Manager graph app invocation...")
                final_state = app.invoke(
                    {"messages": conversation_history, "conversation_id": conversation_id})
                dm_logger.info(
                    f"[CONV_ID: {conversation_id}] Dialog Manager graph app invocation ended.")

                llm_response = final_state['messages'][-1].content

                llm_json_response = json.dumps({
                    "text": llm_response,
                    "conv_id": conversation_id
                }).encode('utf-8')

                dm_logger.info(
                    f"[CONV_ID: {conversation_id}] Publishing message on {topic_to_produce} Kafka topic...")
                producer.produce(
                    topic=topic_to_produce,
                    value=llm_json_response,
                    callback=lambda err, msg: delivery_report(err, msg, conversation_id, dm_logger)
                )

                producer.poll(0)

                consumer.commit(message)
        except Exception as e:
            producer.flush()
            dm_logger.error(f"Error occured: {e}")
            dm_logger.error(traceback.format_exc)


if __name__ == '__main__':
    main()
