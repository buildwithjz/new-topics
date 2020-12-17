FROM python:3.7

RUN pip3 install pymongo

COPY . /app

WORKDIR /app

ENTRYPOINT ["sh","-c","python3 new_topics.py"]
