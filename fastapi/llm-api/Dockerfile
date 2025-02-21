FROM dockerhub.timeweb.cloud/library/python:3.12-slim

ENV DEBIAN_FRONTEND=noninteractive
ENV PYTHONUNBUFFERED=TRUE

RUN apt-get update &&  \
    apt-get install -y  \
    build-essential  \
    python3-dev  \
    libldap2-dev  \
    libsasl2-dev  \
    slapd  \
    ldap-utils  \
    tox  \
    lcov  \
    valgrind  \
    git

COPY requirements.txt .
COPY app /app

RUN pip install --upgrade pip setuptools wheel &&  \
    pip install --no-cache-dir --upgrade -r requirements.txt

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8080"]
