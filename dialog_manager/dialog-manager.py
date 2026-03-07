from typing import TypedDict, Annotated, Sequence
from langchain_core.tools import tool
from langchain_openai import ChatOpenAI
from langchain_core.messages import BaseMessage, SystemMessage
from langgraph.graph.message import add_messages
from langgraph.prebuilt import ToolNode
from langgraph.graph import StateGraph, START, END
from confluent_kafka import Consumer, Producer, KafkaException
import os
import json

# Kafka context
kafka_url = os.getenv('KAFKA_URL', 'localhost:9092')
topic_to_consume = os.getenv('TOPIC_TO_CONSUME', 'user_query')
topic_to_produce = os.getenv('TOPIC_TO_PRODUCE', 'llm_response')


def set_up_kafka_consumer():
    try:
        consumer = Consumer({
            'bootstrap.servers': kafka_url,
            'group.id': 'dialog-manager-group',
            'auto.offset.reset': 'latest'
        })
        consumer.subscribe([topic_to_consume])
        print(f"Subscribed to topic {topic_to_consume}", flush=True)

        return consumer
    except KafkaException as k:
        print(f"Raised exception during subscription: {k}")


def set_up_kafka_producer():
    return Producer({
        'bootstrap.servers': kafka_url
    })


# LLM variables
model_name = os.getenv('MODEL_NAME', "Qwen/Qwen3-4B-Instruct-2507")
model_url = os.getenv('LLM_URL', "http://localhost:8000/v1")


class AgentState(TypedDict):
    messages: Annotated[Sequence[BaseMessage], add_messages]


@tool
def holiday_calendar(date: str) -> str:
    """This is a calendar function that returns the holiday based on the specified date"""

    return "Christmas"


def model_call(state: AgentState) -> AgentState:
    system_prompt = SystemMessage(
        content="You are my AI assistant. Please answer my query to the best of your ability.")
    response = model.invoke([system_prompt] + state['messages'])
    return {'messages': [response]}


def should_continue(state: AgentState):
    last_message = state['messages'][-1]

    return hasattr(last_message, 'tool_calls') and len(last_message.tool_calls) > 0


consumer = set_up_kafka_consumer()
producer = set_up_kafka_producer()

tools = [holiday_calendar]
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
                message = json.loads(message.value().decode('utf-8'))
                user_query = message['text']

                final_state = app.invoke({"messages": [("user", user_query)]})
                llm_response = final_state['messages'][-1].content

                print(llm_response, flush=True)

                llm_json_response = json.dumps({
                    "text": llm_response
                }).encode('utf-8')

                producer.produce(
                    topic=topic_to_produce,
                    value=llm_json_response
                )
        except Exception as e:
            print(f"Error occured during consumer polling: {e}")


if __name__ == '__main__':
    main()
