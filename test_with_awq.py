import time
from openai import OpenAI
from openai.types.responses import ResponseTextDeltaEvent, ResponseCreatedEvent

llm_endpoint = OpenAI(api_key="EMPTY", base_url="http://localhost:8001/v1")
embedding_endpoint = OpenAI(api_key="EMPTY", base_url="http://localhost:8000/v1")

poetry = "Sempre caro mi fu quest'ermo colle, \
e questa siepe, che da tanta parte \
dell'ultimo orizzonte il guardo esclude. \
Ma sedendo e mirando, interminati \
spazi di là da quella, e sovrumani \
silenzi, e profondissima quiete \
io nel pensier mi fingo; ove per poco \
il cor non si spaura. E come il vento \
odo stormir tra queste piante, io quello \
infinito silenzio a questa voce \
vo comparando: e mi sovvien l'eterno, \
e le morte stagioni, e la presente \
e viva, e il suon di lei. Così tra questa \
immensità s'annega il pensier mio: \
e il naufragar m'è dolce in questo mare."

embedding_response = embedding_endpoint.embeddings.create(
   model="text-embedding-embeddinggemma-300m",
   input=poetry
)

print(embedding_response.data[0].embedding)

print("Invio richiesta HTTP...")

start_time = time.time()

llm_response = llm_endpoint.responses.create(
    model="Qwen/Qwen3-4B-Instruct-2507",
    input=poetry + "Puoi parafrasarmi questa poesia?",
    stream=True
)

print(f"Richiesta HTTP inviata! ({(time.time() - start_time):.3f}s)")

first_token = True
for chunk in llm_response:
    if isinstance(chunk, ResponseTextDeltaEvent):
        if first_token:
            print(f"TIME TO FIRST TOKEN: {(time.time() - start_time):.3f}s")
            first_token = False
        print(chunk.delta, end="", flush=True)

print(f"\n\nScript terminato in {(time.time() - start_time):.3f}s")
