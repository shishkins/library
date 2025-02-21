#!/usr/bin/env bash

TRY_LOOP="3"

wait_for_port() {
  local name="$1" host="$2" port="$3"
  local j=0
  while ! nc -z "$host" "$port" >/dev/null 2>&1 < /dev/null; do
    j=$((j+1))
    if [ $j -ge $TRY_LOOP ]; then
      echo >&2 "$(date) - $host:$port still not reachable, giving up"
      exit 1
    fi
    echo "$(date) - waiting for $name... $j/$TRY_LOOP"
    sleep 5
  done
}

case "$1" in
  webserver)
    airflow db migrate
    airflow scheduler &
    airflow users create -r Admin -u admin -e admin@dns-shop.ru -f admin -l admin -p "$AIRFLOW_ADMIN_PASSWORD"
    airflow connections add dwh_minio --conn-type aws --conn-login $MINIO_ACCESS_KEY --conn-password $MINIO_SECRET_KEY --conn-extra '{"endpoint_url":"'$MINIO_ENDPOINT_URL'"}'
    airflow connections add smtp_default --conn-type email --conn-host mail.dns-shop.ru --conn-port 587 --conn-login $AIRFLOW__SMTP__SMTP_USER --conn-password $AIRFLOW__SMTP__SMTP_PASSWORD
#    airflow connections add datahub_rest_default  --conn-type 'datahub-rest' --conn-host $DATAHUB_HOST --conn-password $DATAHUB_TOKEN
    exec airflow webserver
    ;;
  worker|scheduler)
    # Give the webserver time to run initdb.
    sleep 10
    exec airflow "$@"
    ;;
  flower)
    sleep 10
    exec airflow "$@"
    ;;
  version)
    exec airflow "$@"
    ;;
  *)
    # The command is something like bash, not an airflow subcommand. Just run it in the right environment.
    exec "$@"
    ;;
esac