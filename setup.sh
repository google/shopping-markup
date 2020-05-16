# Copyright 2020 Google LLC..
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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
