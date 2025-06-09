#!/usr/bin/env bash

for file in "./sql_testground"/*.sql; do
    psql -f "${file}" > ${file%.sql}.txt
done