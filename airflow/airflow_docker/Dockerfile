FROM python:3.12-slim

ENV DEBIAN_FRONTEND=noninteractive
ENV TERM=linux

ARG AIRFLOW_VERSION=2.10.3
ARG AIRFLOW_PYTHON_VERSION=3.12
ARG CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${AIRFLOW_PYTHON_VERSION}.txt"
ENV AIRFLOW_HOME=/usr/local/airflow

# INSTALL SYSTEM REQUIREMENTS
RUN apt-get update -yqq \
    && apt-get upgrade -yqq \
    && apt-get install -yqq \
        libssl-dev \
        libsasl2-2 \
        libsasl2-modules \
        libsasl2-dev \
        unixodbc \
        freetds-bin \
        freetds-dev \
        build-essential \
        python3-dev \
        curl \
        git \
        ldap-utils \
        libldap2-dev \
        gcc \
        libkrb5-dev \
    && useradd -ms /bin/bash -d ${AIRFLOW_HOME} airflow

# INSTALL PYTHON REQUIREMENTS
COPY ./requirements.txt /requirements.txt
RUN pip install -U pip setuptools wheel \
    && pip install -r /requirements.txt

# INSTALL AIRFLOW
RUN pip install "apache-airflow[postgres,odbc,jdbc,ssh,http,amazon,virtualenv]==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"

COPY script/entrypoint.sh /entrypoint.sh
COPY config/airflow.cfg ${AIRFLOW_HOME}/airflow.cfg
COPY config/webserver_config.py ${AIRFLOW_HOME}/webserver_config.py

RUN chmod +x /entrypoint.sh
RUN chown -R airflow: ${AIRFLOW_HOME}

EXPOSE 8180

USER airflow
WORKDIR ${AIRFLOW_HOME}
ENTRYPOINT ["/entrypoint.sh"]
CMD ["webserver"]
