# Royal Blue

## Table of Contents
1. [Introduction](#introduction)  
2. [Project Description](#project-description)  
3. [Requirements](#requirements)  
4. [Installation Instructions](#installation-instructions)  
5. [Running Python Scripts Locally](#running-python-scripts-locally)  
6. [Makefile Commands](#makefile-commands)  

---

## Introduction

Welcome to **Royal Blue**! Your go-to toolkit for streamlined data workflows, automated infrastructure, using Python and Terraform with best practices for testing, linting, formatting, and deployment.

 Think of it as the secret sauce that keeps your ETL and cloud setup running smoothly without the hassle ;)

---

## Project Description

This repository contains Python scripts for ETL pipelines, infrastructure as code using Terraform, and handy utility scripts, all orchestrated to give you robust, scalable data processes. It leverages AWS services such as Lambda and S3, and focuses on maintainable and testable code with continuous integration support.

---

## Requirements

To keep everything consistent, we use [uv](https://docs.astral.sh/uv/) for Python environment and dependency management. It’s a powerhouse that handles installations and script execution seamlessly.

- Make sure `uv` is installed. Follow the [official guide](https://docs.astral.sh/uv/getting-started/installation/).
- For an even smoother experience, enable shell autocompletion as explained [here](https://docs.astral.sh/uv/getting-started/installation/#shell-autocompletion).

---

## Tech Stack

**Programming Language & Runtime**  
- Python 3.13+  

**Core Dependencies**  
- `orjson` (fast JSON serialization/deserialization)  
- `psycopg[binary]` (PostgreSQL database adapter)  

**Development Dependencies**  
- `pytest` and related plugins for testing and coverage  
- `ruff` for linting and formatting  
- `bandit` for security scanning  
- `moto` for AWS service mocking during tests  
- `boto3` for AWS SDK integration  
- `pandas` for data manipulation  
- `pyarrow` for Parquet file handling  
- `ipykernel` for Jupyter notebook support  

**Database**  
- PostgreSQL (used locally for testing and integration, SQL scripts for test data and schema setup)  

**AWS**  
- Lambda, S3, and related AWS services accessed via `boto3`  

**Utilities & Tooling**  
- `uv` for managing Python environments, dependencies, and running scripts  
- Makefile for task automation (testing, linting, formatting, deployment)  

**Local Testing Scripts**  
- Bash script to run SQL test files against the local PostgreSQL database and capture output for validation.  

---

## Installation Instructions

1. Clone the repository to your local machine:

    ```bash
    git clone https://github.com/theorib/royal-blue.git
    ```

2. Change directory into the cloned repository:

    ```bash
    cd royal-blue
    ```

3. Create a `.env` file at the root, based on the `.env.example` provided. Ensure essential variables like those starting with `DB_` are set. Others, like S3 bucket names, are only needed if you plan to run scripts locally.

      ```bash
            TOTESYS_DB_USER=some_user_abc
            TOTESYS_DB_PASSWORD=some_password_xyz
            TOTESYS_DB_HOST=host.something.com
            TOTESYS_DB_DATABASE=database_name
            TOTESYS_DB_PORT=0000
            INGEST_ZONE_BUCKET_NAME=some_bucket_name
            PROCESS_ZONE_BUCKET_NAME=some_bucket_name
            LAMBDA_STATE_BUCKET_NAME=another_bucket_name
    ```

4. Run the magic words (this installs dependencies, runs tests and checks):

    ```bash
    make setup
    ```

---

## Running Python Scripts Locally

With `uv` managing the environment, running your scripts is clean and consistent. Here's how to start:


1. Activate the Python virtual environment:

    ```bash
    source .venv/bin/activate
    ```

2. Set the `PYTHONPATH` environment variable to the current directory:

    ```bash
    export PYTHONPATH=$(pwd)
    ```

3. Point `uv` to the `.env` file for environment variables:

    ```bash
    export UV_ENV_FILE=.env
    ```

4. Run Python scripts or tests using `uv run`:

    ```bash
    uv run src/lambdas/extract_lambda.py
    ```

    or run tests:

    ```bash
    uv run pytest
    ```

---

## Makefile Commands

Use these main commands for common tasks:

| Command               | Description                                         |
|-----------------------|-----------------------------------------------------|
| `make setup`          | Complete installation and validation (sync, build, checks) |
| `make test`           | Run all tests                                      |
| `make lint`           | Run linter and auto-fix code issues                 |
| `make fmt`            | Format source code                                  |
| `make fix`            | Run formatter and linter                            |
| `make safe`           | Run security scans                                 |
| `make sync`           | Install Python dependencies                         |
| `make help`           | Show all available make commands                    |

For a full list of commands and detailed descriptions, run:

```bash
make help
```