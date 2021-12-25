# syntax=docker/dockerfile:1.0.0-experimental
  
FROM alpine

RUN apk add --update \
    python3 \
    python3-dev \
    py3-pip \
    build-base \
    libffi-dev \
    musl-dev \
    gcc \
    libevent-dev \
  && rm -rf /var/cache/apk/*

COPY ./requirements.txt /requirements.txt

RUN pip install -r /requirements.txt

COPY ./src/metaphor/ /app/metaphor/
COPY ./src/server.py /app/
COPY ./bin/metaphor /bin/

WORKDIR /app/

ENV PYTHONPATH=/app/

EXPOSE 8000

ENTRYPOINT ["gunicorn", "server:app", "--timeout", "6000", "--bind=0.0.0.0:8000", "--access-logfile=-"]
