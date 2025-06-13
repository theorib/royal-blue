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

Think of it as the secret sauce that keeps your ETL and cloud setup running smoothly without the hassle. ;)

---

## Project Description

Jokes aside, the purpose of this repository is to build an entire ETL (Extract, Load, Transform) data pipeline in AWS (Amazon Web Services).

Extracting data from an OLTP (Online Transaction Processing Database) PostgreSQL database and loading it into an OLAP (Online Analytical Processing Database) database.

The data  is transformed from transactional day to day business data into a Data Analisys ready format, suitable for multiple Business Inteligence purposes.

It uses [Python](https://www.python.org) as the main programming language, followed by [Terraform](https://www.hashicorp.com/en/products/terraform) for infrastructure as code. It also uses Bash and SQL Scripts to help with build processes and integration testing, and has a full-featured Makefile for convenience.
    

---

## Requirements

- Make sure [uv](https://docs.astral.sh/uv/) is installed by following the [official guide](https://docs.astral.sh/uv/getting-started/installation/). We use it to manage Python environments, dependencies, to run scripts, and for our build process.
- For an even smoother experience, enable uv command shell autocompletion as explained [here](https://docs.astral.sh/uv/getting-started/installation/#shell-autocompletion).

- You will also need to install the [latest version of Python](https://www.python.org/downloads/) (3.13.3 at the time of this writting).

- For local development, you will need to install the [AWS CLI](https://aws.amazon.com/cli/).

---

## Tech Stack

**Programming Language & Runtime**  
- Python 3.13.3+  

**Core Python Dependencies**  
- [`psycopg3` (PostgreSQL database adapter)](https://www.psycopg.org/psycopg3/docs/)  
- [`boto3` for AWS SDK integration](https://boto3.amazonaws.com/v1/documentation/api/latest/index.html)
- [`pandas` for data manipulation](https://pandas.pydata.org/docs/)
- [`pyarrow` for Parquet file handling](https://arrow.apache.org/docs/python/)
- [`orjson` (fast JSON serialisation/deserialization)](https://github.com/ijl/orjson) 

**Development Dependencies**  
- [`pytest` and related plugins for testing and coverage](https://docs.pytest.org/en/stable/)  
- [`ruff` for linting and formatting](https://docs.astral.sh/ruff/)
- [`moto` for AWS service mocking during tests](https://docs.getmoto.org/en/latest/docs/getting_started.html)
- [`bandit` for vulnerability and security scanning of source code](https://bandit.readthedocs.io/en/latest/)
- [`ipykernel` for VS Code Jupyter notebook support](https://ipykernel.readthedocs.io/en/stable/)

**Databases**  
- [PostgreSQL](https://www.postgresql.org) (used locally for integration tests, and being the database on both sides of the pipeline).

**AWS**  
- Lambda, S3, Step Functions, IAM, Cloudwatch, SNS Email alerts, etc. All accessed using [`boto3`](https://boto3.amazonaws.com/v1/documentation/api/latest/index.html) deployed with [Terraform](https://www.hashicorp.com/en/products/terraform).

**Utilities & Tooling**  
- [`uv`](https://docs.astral.sh/uv/) for managing Python environments, dependencies, and running scripts
- `Makefile` for task automation (testing, linting, formatting, deployment).

**Local Testing Scripts**  
- Bash scripts to run SQL test files against the local PostgreSQL database and capture output for validation.

---

## Installation Instructions

1. Clone or fork this repository and download it to your local machine:

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
    DATAWAREHOUSE_DB_USER=some_user_abc
    DATAWAREHOUSE_DB_PASSWORD=some_password_xyz
    DATAWAREHOUSE_DB_HOST=host.something.com
    DATAWAREHOUSE_DB_DATABASE=database_name
    DATAWAREHOUSE_DB_PORT=0000
    
    # For local integration tests only:
    INGEST_ZONE_BUCKET_NAME=some_bucket_name
    PROCESS_ZONE_BUCKET_NAME=some_bucket_name
    LAMBDA_STATE_BUCKET_NAME=another_bucket_name
    ```

4. If you forked this repository and want CI/CD to work as intended, you will have to create GitHub Secrets for the above environment variables (except for the local integration ones). 

5. Run the magic words (this installs dependencies, runs tests and checks):

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

3. Point `uv` to your local `.env` file so that environment variables are available to running scripts:

    ```bash
    export UV_ENV_FILE=.env
    ```

4. Run Python scripts or tests using `uv run`:

    ```bash
    uv run src/lambdas/extract_lambda.py
    ```

    Example on how to run tests:

    ```bash
    uv run pytest
    ```

---

## Available Makefile Commands

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
