#!/bin/bash

set -e

host="db"
port="5432"

>&2 echo "!!!!!!!! Check conteiner_a for available !!!!!!!!"

until nc -z $host $port; do
  >&2 echo "Conteiner_A is unavailable - sleeping"
  sleep 1
done

>&2 echo "Conteiner_A is up - executing command"

exec python main.py