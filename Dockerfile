FROM python:3.7

RUN pip3 install pymongo
RUN pip3 install requests

COPY . /app

WORKDIR /app

ENTRYPOINT ["sh","-c","python3 new_topics.py"]
