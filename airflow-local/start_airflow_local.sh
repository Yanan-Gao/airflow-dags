#!/bin/bash

docker compose -f ./airflow-local/docker-compose.yml up -d --build --remove-orphans

while ! curl --fail --silent --head http://localhost:3000; do
  sleep 1
done

open http://localhost:3000
