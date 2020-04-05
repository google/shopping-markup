#!/bin/bash
# MarkUp setup script.

set -e

VIRTUALENV_PATH=$HOME/"markup-venv"

# Create virtual environment with python3
if [[ ! -d "${VIRTUALENV_PATH}" ]]; then
  virtualenv -p python3 "${VIRTUALENV_PATH}"
fi


# Activate virtual environment.
source ~/markup-venv/bin/activate

# Install dependencies.
pip install -r requirements.txt

# Setup cloud environment.
PYTHONPATH=src/plugins:$PYTHONPATH
export PYTHONPATH
python cloud_env_setup.py "$@"
