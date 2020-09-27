FROM python:slim

WORKDIR /home

COPY requirements.txt .
COPY jikan-bq-schema.json .
COPY etl-jikan.py .
COPY entrypoint.sh .
COPY ani-project-key.json .

RUN pip install -r requirements.txt

CMD ["bash", "./entrypoint.sh"]