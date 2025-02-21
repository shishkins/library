# Airflow

Образ сервиса в 2-х контейнероах: Airflow с Local Executor и системная БД (PostgreSQL)

Поддерживается LDAP авторизация

Характеристики:

* Python 3.12
* Airflow 2.9.2
* Postgres 15

Для корректной работы необходимо заполнить переменные окружения в `.env`

FAQ:

* Все необходимые зависимости указываются в [requirements.txt](requirements.txt)
* DAG'и хранятся в папке [dags](dags)
* Логи Airflow хранятся в папке [logs](logs) или в S3
