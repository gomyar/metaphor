# syntax=docker/dockerfile:1.0.0-experimental
  
FROM alpine:3.20.3

RUN apk add --update \
    python3 \
    python3-dev \
    py3-pip \
    build-base \
    libffi-dev \
    musl-dev \
    gcc \
    libevent-dev \
    git \
  && rm -rf /var/cache/apk/*

COPY ./requirements.txt /requirements.txt

RUN python3 -m venv /opt/venv

RUN /opt/venv/bin/pip install -r /requirements.txt

COPY ./src/metaphor/ /app/metaphor/
COPY ./src/server.py /app/
COPY ./bin/metaphor /bin/

WORKDIR /app/

ENV PYTHONPATH=/app/

EXPOSE 8000

ENV PATH="/opt/venv/bin:$PATH"

ENTRYPOINT ["gunicorn", "-k", "geventwebsocket.gunicorn.workers.GeventWebSocketWorker", "server:app", "--timeout", "6000", "--bind=0.0.0.0:8000", "--access-logfile=-"]
