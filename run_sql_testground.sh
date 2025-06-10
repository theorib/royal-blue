#!/usr/bin/env bash

for file in "./sql_local_tests"/*.sql; do
    psql -f "${file}" > ${file%.sql}.txt
done