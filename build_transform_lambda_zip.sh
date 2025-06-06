#!/bin/bash

# Create directories if they don't exist
mkdir -p build
mkdir -p dist
mkdir -p build/transform_lambda/

# Copy all files from src into build/extract_lambda/src so they can be zipped.
rsync -av --exclude='__pycache__' --exclude='*.pyc' --exclude='.pytest_cache' --exclude='.git' --exclude='*.DS_Store' ./src/ ./build/transform_lambda/src/

# zip extract lambda source code
cd ./build/transform_lambda && zip -r ../../dist/transform_lambda.zip . && cd ../..

