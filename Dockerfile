FROM ubuntu:20.04

ENV DEBIAN_FRONTEND noninteractive

RUN apt-get update && apt-get -y upgrade && \
    apt-get -y install python3.8 && \
    apt update && apt install python3-pip -y

# Method1 - installing mdbtools
RUN apt-get install -y mdbtools

ARG CACHEBUST=1

COPY requirements.txt .

RUN pip install -r requirements.txt