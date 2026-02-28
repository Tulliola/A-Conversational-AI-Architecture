from openai import OpenAI

llm_endpoint = OpenAI(api_key="none", base_url="http://llm-service:80/v1")

response = llm_endpoint.chat.completions.create(
    model="Qwen/Qwen3-4B-Instruct-2507",
    messages=[
        {"role": "system", "content": "Sei un insegnante di Ingegneria del Software."},
        {"role": "user", "content": "Qual è la differenza Test Black Box e White Box?"}
    ],
    max_tokens=500
)

print(response.choices[0].message.content)
