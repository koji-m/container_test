FROM python:3.9-slim

RUN pip install boto3 SQLAlchemy PyMySQL

COPY extract.py /app/extract.py

WORKDIR /app

CMD python extract.py
