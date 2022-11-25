FROM ubuntu:20.04

ARG XL_IDP_PATH_DOCKER
ARG CACHEBUST=1

ENV DEBIAN_FRONTEND noninteractive
ENV XL_IDP_PATH_DOCKER=$XL_IDP_PATH_DOCKER

RUN apt-get update && apt-get -y upgrade && \
    apt-get -y install python3.8 && \
    apt update && apt install python3-pip -y

# Method1 - installing mdbtools
RUN apt-get install -y mdbtools

RUN chmod -R 777 $XL_IDP_PATH_DOCKER

COPY requirements.txt .

RUN pip install -r requirements.txt
