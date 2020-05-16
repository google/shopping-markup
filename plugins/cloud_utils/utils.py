# coding=utf-8
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

# Lint as: python3
# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""A utility module for cloud_utils package.

  This module implements the common methods required for different modules under
  cloud_utils package.
"""

import time
from typing import Any, Dict, Text

import logging
import apiclient
from googleapiclient import errors
from googleapiclient import http

from google.api_core import retry


# Number of seconds to wait before re-checking for operation status.
_WAIT_FOR_OPERATION_SLEEP_SECONDS = 10
_RETRIABLE_STATUS_CODES = (
    429,  # Too Many Requests
    500,  # Internal Server Error
    503)  # Service Unavailable


class Error(Exception):
  """A generic error thrown for any exception in utils module."""
  pass


def _is_retriable_http_error(error: errors.HttpError) -> bool:
  """Checks if HttpError is in _RETRIABLE_STATUS_CODES.

  This function requires HttpError to have a valid response.

  Args:
    error: The http error to check.

  Returns:
    True if HttpError is retriable, otherwise False.
  """
  if error.__dict__['resp'].status in _RETRIABLE_STATUS_CODES:
    return True
  return False


@retry.Retry(predicate=_is_retriable_http_error)
def execute_request(request: http.HttpRequest) -> Any:
  """Executes an HTTP request and return its response.

  This method executes an HTTP request and retries if the response error code is
  capable of being retried. Refer to `retry_utils.py` module to see the list of
  error codes for which the retry will be attempted.

  Args:
    request: HTTP request to be executed.

  Returns:
    response: Response from the HTTP request.
  """
  response = request.execute()
  return response


def wait_for_operation(operation_client: apiclient.discovery.Resource,
                       operation: Dict[Text, Any]) -> None:
  """Waits for the completion of operation.

  This method retrieves operation resource and checks for its status. If the
  operation is not completed, then the operation is re-checked after
  `_WAIT_FOR_OPERATION_SLEEP_SECONDS` seconds.

  Args:
    operation_client: Client with methods for interacting with the operation
      APIs. The `build_service_client` method from `cloud_auth` module can be
      used to build the client.
    operation: Resource representing long running operation.

  Raises:
    Error: If the operation is not successfully completed.
  """
  while True:
    request = operation_client.get(name=operation['name'])
    updated_operation = execute_request(request)
    if updated_operation.get('done'):
      logging.info(f'Operation {operation["name"]} successfully completed.')
      return
    if updated_operation.get('error'):
      logging.info(
          f'Operation {operation["name"]} failed to complete successfully.')
      raise Error(
          f'Operation {operation["name"]} not completed. Error Details - '
          f'{updated_operation["error"]}')
    logging.info(
        f'Operation {operation["name"]} still in progress. Sleeping for '
        f'{_WAIT_FOR_OPERATION_SLEEP_SECONDS} seconds before retrying.')
    time.sleep(_WAIT_FOR_OPERATION_SLEEP_SECONDS)
