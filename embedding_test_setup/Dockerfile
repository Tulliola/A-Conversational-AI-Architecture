FROM python:3.13.11-alpine

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY embedding_test.py .

CMD [ "python", "./embedding_test.py" ]