FROM python:3.13.11-alpine

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY llm_test.py .

CMD [ "python", "./llm_test.py" ]