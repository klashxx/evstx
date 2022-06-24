#!/usr/bin/env bash

# MUST RUN IN THE BROKER CONTAINER

while true; do
  id=$(cat /proc/sys/kernel/random/uuid|tr -d -)
  timestamp=$(date +%s)
  echo "{\"id\": \"$id\", \"timestamp\": $timestamp}" > /tmp/tmp.json
  kafka-console-producer -bootstrap-server broker:9092 --topic evstx < /tmp/tmp.json
  sleep 10
done
