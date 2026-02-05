# Obiettivo:
# 1. Fornire audio ad un ASR;
# 2. Ricevere la stringa restituita dall'ASR agent;
# 3. Passare questa stringa ricevuta dall'ASR agent al TTS agent;
# 4. Ricevere l'output
from openai import OpenAI
import time
import os

llm_client = OpenAI(api_key="none", base_url=os.getenv("LLM_URL"))
asr_client = OpenAI(api_key="none", base_url=os.getenv("ASR_URL"))
tts_client = OpenAI(api_key="none", base_url=os.getenv("TTS_URL"))

SHARED_DIR = "/shared_data"
input_path = os.path.join(SHARED_DIR, "question_test.mp3")
file_audio = open(input_path, "rb")

print("[ASR] Starting transcription...")
start_time = time.time()

transcription = asr_client.audio.transcriptions.create(
    model="small",
    file=file_audio,
    response_format="text"
)

end_time = time.time()
print("[ASR] Ending transcription...")
print(f"[ASR] Time elapsed: {end_time - start_time} seconds\n")

print(f"\n\n{transcription}")

print("[LLM] Starting completion...")
start_time = time.time()

response_from_llm = llm_client.chat.completions.create(
    model="Qwen/Qwen3-4B-Instruct-2507",
    messages=[
        {'role': 'system',
            'content': 'You are a funny agent. Explain everything you have been asked in terms of Red Dead Redemption 2.'},
        {'role': 'user', 'content': transcription}
    ],
    stream=True,
    extra_body={
        'options': {
            # Riduce il numero di token conversazionali che vengono usati come contesto per la
            # memorizzazione dell'intera conversazione.
            'num_ctx': 1024
        }
    }
)

full_text = ""

for chunk in response_from_llm:
    curr_chunk = chunk.choices[0].delta.content
    if curr_chunk:
        print(curr_chunk, end="", flush=True)
        full_text += curr_chunk

end_time = time.time()
print("[LLM] Ending completion...")
print(f"[LLM] Time elapsed: {end_time - start_time} seconds\n")

print("\n[TTS] Starting creating audio...")
start_time = time.time()

output_path = os.path.join(SHARED_DIR, "answer_test.mp3")

with tts_client.audio.speech.with_streaming_response.create(
    model="kokoro",
    input=full_text,
    voice="af_bella"
) as response:
    response.stream_to_file(output_path)

end_time = time.time()
print("[TTS] Audio created...")
print(f"[TTS] Time elapsed: {end_time - start_time} seconds\n")
