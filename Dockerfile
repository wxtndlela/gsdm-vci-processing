FROM python:3.8-slim-buster

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --upgrade pip
COPY requirements.txt /app/requirements.txt
RUN pip install -r /app/requirements.txt

COPY . /app
WORKDIR /app

CMD ["python", "vci.py"]