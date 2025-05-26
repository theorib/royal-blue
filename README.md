# royal-blue

## Table of contents
1. [Requirements](#requirements)
2. [Installation instructions](#installation-instructions)

## Requirements
We are using [uv](https://docs.astral.sh/uv/) to manage python installation, python dependencies, and to run scripts. It provides a single source of truth for all project related dependencies. 
1. **`uv`**: Make sure you have `uv` installed by following the [installation instructions](https://docs.astral.sh/uv/getting-started/installation/) for your operating system.
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
3. Install dependencies with `uv`:
    ```bash
    uv sync
    ```
4. Run any files or commands, example:
    ```bash
    uv run pytest
    ```

5. Alternatively go the traditional route:
    1. Activate your virtual environment:
    ```bash
    source .venv/bin/activate
    ```
    2. Export the current directory as your `PYTHONPATH`
    ```bash
    export PYTHONPATH=$(pwd)
    ```