FROM python:3.13.11-alpine

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY test.py .

CMD [ "python", "./test.py" ]