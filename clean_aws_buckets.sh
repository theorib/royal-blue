#!/bin/bash
aws s3 rm s3://ingest-zone-20250610220532948400000006 --recursive
aws s3 rm s3://process-zone-20250610220532948300000005 --recursive
aws s3 rm s3://lambda-state-20250610220532948300000004 --recursive