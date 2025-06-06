#!/bin/bash

mkdir -p terraform
touch terraform/terraform.auto.tfvars
# create a 
cat .env | sed 's/=/ = "/' | sed 's/$/"/' > terraform/terraform.auto.tfvars