#!/bin/bash

# Create directories if they don't exist
mkdir -p build
mkdir -p dist
mkdir -p build/extract_lambda/

# Copy all files from src into build/extract_lambda/src so they can be zipped.
rsync -av --exclude='__pycache__' --exclude='*.pyc' --exclude='.pytest_cache' --exclude='.git' --exclude='*.DS_Store' ./src/ ./build/extract_lambda/src/

# zip extract lambda source code
cd ./build/extract_lambda && zip -r ../../dist/extract_lambda.zip . && cd ../..

