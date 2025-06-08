# royal-blue

## Table of contents
1. [Introduction](#introduction)
2. [Project Description](#project-description)
3. [Requirements](#requirements)
4. [Installation instructions](#installation-instructions)
5. [Running python scripts locally](#running-python-scripts-locally)
6. [Aditional conveniences using `make`](#aditional-conveniences-using-make)

## Introduction

## Project Description

## Requirements
We are using [uv](https://docs.astral.sh/uv/) to manage python installation, python dependencies, and to run scripts. It provides a single source of truth for all project related dependencies. 
- Make sure you have `uv` installed by following the [installation instructions](https://docs.astral.sh/uv/getting-started/installation/) for your operating system.
- Optionally (recommended), [configure uv shell autocompletion](https://docs.astral.sh/uv/getting-started/installation/#shell-autocompletion) for your terminal shell of choice.


## Installation instructions
1. Clone this repository into your folder of choice:
    Open a terminal prompt and navigate to the desired destination folder (example: `~/projects/`) then type:
    ```bash
    git clone https://github.com/theorib/royal-blue.git
    ```
2. Change your terminal current directory to the directory created by `git clone` (by default it will be `royal-blue`):
    ```bash
    cd royal-blue
    ```
3. At the root of the repository, create a `.env` file following the example from this repository's [`.env.example`](.env).
    - Note that the essential environment variables here are the ones starting with `DB_` the other ones, related to S3 bucket names, are only necessary if you are trying to run python scripts locally with `uv run some_file.py` for example.

4. Run the installation script. It will install all dependencies, run tests and checks to see if your repository is working. Note that you will need to create your `.env` file first:
    ```bash
    make setup
    ```

## Running python scripts locally
Since we are using `uv` to manage this project, instead of running scripts with the `python script-name.py` we run then using the `uv run` command.

1. Activate the virtual environment:
```bash
source .venv/bin/activate
```

2. Export the current directory to your `PYTHONPATH`:
```bash
export PYTHONPATH=$(pwd)
```

3. Tell `uv` where to find your project's environment variables from your `.env` file so they are available when running scripts locally: 
```bash
export UV_ENV_FILE=.env  
```

4. Run scripts using `uv run`, examples:
```bash
uv run src/lambdas/extract_lambda.py
```
```bash
uv run pytest
```

## Aditional conveniences using `make`
To get a list of all `make` commands available in this project and a brief explanation of what they do type in your terminal:
```bash
make help
```