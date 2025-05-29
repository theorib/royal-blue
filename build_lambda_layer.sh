#!/bin/bash

# Create directories if they don't exist
mkdir -p build
mkdir -p dist

# Export requirements
uv export --frozen --no-dev --no-editable -o ./build/layer/python/requirements.txt

# Install dependencies
uv pip install \
    --no-installer-metadata \
    --no-compile-bytecode \
    --python-platform x86_64-manylinux2014 \
    --python 3.13.3 \
    --target build/layer/python \
    -r build/layer/python/requirements.txt

# Create zip with dependencies
# cd build/layer && zip -r ../../dist/layer.zip . && cd ../..

# Remove temporary build directory
# rm -rf build