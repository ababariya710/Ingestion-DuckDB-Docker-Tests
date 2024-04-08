FROM python:3.11-slim as base

ENV LANG C.UTF-8
ENV LC_ALL C.UTF-8


FROM base AS python-deps

RUN apt-get update && apt-get install -y --no-install-recommends gcc libpq-dev curl g++
RUN pip install --upgrade pip
RUN pip install poetry

RUN mkdir -p /home/dataeng
WORKDIR /home/dataeng
