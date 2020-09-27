FROM python:slim

WORKDIR /home

COPY requirements.txt .
COPY jikan-bq-schema.json .
COPY etl-jikan.py .

RUN pip install -r requirements.txt

CMD ['python', './etl-jikan.py']