from typing import TypedDict, Annotated, Sequence
from langchain_core.tools import tool
from langchain_openai import ChatOpenAI
from langchain_core.messages import BaseMessage, SystemMessage
from langgraph.graph.message import add_messages
from langgraph.prebuilt import ToolNode
from langgraph.graph import StateGraph, START, END
from confluent_kafka import Consumer, Producer, KafkaException
from neo4j import GraphDatabase
from openai import OpenAI
import os
import json
import sys
import traceback

# Kafka context
kafka_url = os.getenv('KAFKA_URL', 'localhost:9092')
topic_to_consume = os.getenv('TOPIC_TO_CONSUME', 'user_query')
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
        print(f"Subscribed to topic {topic_to_consume}", flush=True)

        return consumer
    except KafkaException as k:
        print(f"Raised exception during subscription: {k}")
        sys.exit(1)


def set_up_kafka_producer():
    return Producer({
        'bootstrap.servers': kafka_url
    })


# Neo4j variables
db_uri = os.getenv('DB_URL', 'neo4j://db-headless-service:7687')
db_auth = (os.getenv('DB_AUTH_USER', 'neo4j'),
           os.getenv('DB_AUTH_PASSWORD', 'password'))


def get_conversation_history(session_id):
    cypher_query = """
    MATCH (c:CONVERSATION {conv_id: $session_id})-[:STARTS_WITH|FOLLOWED_BY*0..]->(u:UTTERANCE)
    RETURN 
        CASE 
            WHEN u:USER THEN 'user'
            WHEN u:SYSTEM THEN 'ai' 
            ELSE 'unknown' 
        END AS role, 
        u.text AS text
    """

    with GraphDatabase.driver(db_uri, auth=db_auth) as driver:
        records, _, _ = driver.execute_query(
            cypher_query,
            session_id=session_id,
            database_="neo4j"
        )
        return [(record["role"], record["text"]) for record in records]


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


def get_embedding(text):
    return embedding_model.embeddings.create(
        input=[text.replace('\n', ' ')],
        model=embedding_name).data[0].embedding


def get_full_context(conversation_id, user_query):
    system_prompt = (
        "system",
        "Sei un assistente olografico personale specializzato. "
        "Rispondi alle domande al meglio delle tue capacità. "
        "Se l'utente fa domande specifiche sull'epilessia, DEVI assolutamente utilizzare il tool 'retrieval_augmented_generation' per cercare le informazioni prima di rispondere. "
        "Se chiede informazioni sulle date, usa il tool 'holiday_calendar'.")
    past_conversation_history = get_conversation_history(
        conversation_id)
    
    if len(past_conversation_history) > 4:
        past_conversation_history = past_conversation_history[-4:]

    current_conversation_history = [("user", user_query)]
    return [system_prompt] + \
        past_conversation_history + current_conversation_history


class AgentState(TypedDict):
    messages: Annotated[Sequence[BaseMessage], add_messages]


@tool
def retrieval_augmented_generation(user_query: str) -> list[str]:
    """
    DEVE essere invocato per TUTTE le domande relative all'epilessia.
    Cerca nel database vettoriale informazioni cliniche e mediche sull'epilessia.
    
    Args:
        user_query: La domanda esatta o il concetto sull'epilessia cercato dall'utente.
    """

    user_query_embedding = get_embedding(user_query)

    cypher_query = """
        CALL db.index.vector.queryNodes("embeddingIndex", 3, $embedding)
        YIELD node, score
        WITH node AS emb, score AS affinity ORDER BY affinity DESC
        MATCH (c:CHUNK)-[:HAS_QUESTION|HAS_STATEMENT]-(k:QUESTION|STATEMENT)-[:HAS_EMBEDDING]-(emb:EMBEDDING)
        RETURN c.text AS text
    """

    with GraphDatabase.driver(db_uri, auth=db_auth) as driver:
        records, _, _ = driver.execute_query(
            cypher_query,
            embedding=user_query_embedding,
            database_="neo4j"
        )
        return [(record["text"]) for record in records]


@tool
def holiday_calendar(date: str) -> str:
    """
    DEVE essere invocato ogni qualvolta l'utente fa riferimento ad una data e vuole conoscere la festività che cade in quella data.
    Restituisce la festività che cade in quella data specificata dall'utente.

    Args:
        date: la data a cui l'utente fa riferimento.
    """

    return "Natale"


def model_call(state: AgentState) -> AgentState:
    response = model.invoke(state['messages'])
    return {'messages': [response]}


def should_continue(state: AgentState):
    last_message = state['messages'][-1]

    return hasattr(last_message, 'tool_calls') and len(last_message.tool_calls) > 0


consumer = set_up_kafka_consumer()
producer = set_up_kafka_producer()

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
    while True:
        try:
            message = consumer.poll(timeout=1.0)

            if message and message.error():
                print(f"Consumer error: {message}", flush=True)
            elif message:
                payload = json.loads(message.value().decode('utf-8'))
                user_query = payload['text']
                conversation_id = payload['conv_id']

                if 'text' not in payload and 'conv_id' not in payload:
                    print(f"[ERROR] Malformed message {payload}", flush=True)
                    continue

                print(f"Received message: {payload}", flush=True)

                full_context = get_full_context(conversation_id, user_query)

                final_state = app.invoke({"messages": full_context})
                llm_response = final_state['messages'][-1].content

                print(llm_response, flush=True)

                llm_json_response = json.dumps({
                    "text": llm_response,
                    "conv_id": conversation_id
                }).encode('utf-8')

                producer.produce(
                    topic=topic_to_produce,
                    value=llm_json_response
                )

                consumer.commit(message)
        except Exception as e:
            producer.flush()
            print(f"Error occured during consumer polling: {e}")
            traceback.print_exc()


if __name__ == '__main__':
    main()
