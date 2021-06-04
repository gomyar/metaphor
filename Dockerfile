FROM ubuntu:20.10

RUN apt update -y
RUN apt install -y \
    python3 \
    python3-dev \
    python3-pip \
    build-essential \
    libffi-dev \
  && pip install virtualenv

COPY ./requirements.txt /requirements.txt

RUN pip install -r /requirements.txt

COPY ./src/metaphor/ /app/metaphor/
COPY ./src/server.py /app/

WORKDIR /app/

EXPOSE 8000

ENTRYPOINT ["gunicorn", "server:app", "--timeout", "6000", "--bind=0.0.0.0:8000", "--access-logfile=-"]
