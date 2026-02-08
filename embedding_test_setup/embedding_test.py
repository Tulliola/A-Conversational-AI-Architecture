from openai import OpenAI

endpoint = OpenAI(
    api_key="none", base_url="http://embedding-gemma-service:80/v1")

embedding = endpoint.embeddings.create(
    model="text-embedding-embeddinggemma-300m",
    input="Che cos'è l'epilessia?",
    encoding_format="float"
)

print(embedding.data[0].embedding)
