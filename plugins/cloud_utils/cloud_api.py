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

"""Wrapper for Cloud APIs."""

import json
from typing import Any, List

import logging
from googleapiclient import errors
import requests.exceptions

from google.auth.transport.requests import AuthorizedSession

from plugins.cloud_utils import cloud_auth
from plugins.cloud_utils import utils

_SERVICE_URL = 'https://serviceusage.googleapis.com/v1/projects'


class Error(Exception):
  """A generic error thrown for any exceptions in cloud_api module."""
  pass


class CloudApiUtils(object):
  """CloudApiUtils class provides methods to manage Cloud Services.

  This class manages Cloud APIs within a single GCP project.

  Typical usage example:
       >>> ca_utils = CloudApiUtils('project_id',
                                    'service_account_key_file.json')
       >>> ca_utils.enable_apis(['storage-component.googleapis.com'])
  """

  def __init__(self,
               project_id: str,
               service_account_key_file: str = None) -> None:
    """Initialise new instance of CloudApiUtils.

    Args:
      project_id: GCP project id.
      service_account_key_file: Optional. File containing service account key.
        If not passed the default credential will be used. There are following
        ways to create service accounts:
          1. Use `create_service_account_key` method from `cloud_auth` module.
          2. Use `gcloud` command line utility as documented here -
             https://cloud.google.com/iam/docs/creating-managing-service-account-keys
    """
    self.client = cloud_auth.build_service_client('serviceusage',
                                                  service_account_key_file)
    self.project_id = project_id

  def enable_apis(self, apis: List[str]) -> None:
    """Enables multiple Cloud APIs for a GCP project.

    Args:
      apis: The list of APIs to be enabled.

    Raises:
        Error: If the request was not processed successfully.
    """
    parent = f'projects/{self.project_id}'
    request_body = {'serviceIds': apis}
    try:
      request = self.client.services().batchEnable(
          parent=parent, body=request_body)
      operation = utils.execute_request(request)
      utils.wait_for_operation(self.client.operations(), operation)
    except errors.HttpError:
      logging.exception('Error occurred while enabling Cloud APIs.')
      raise Error('Error occurred while enabling Cloud APIs.')


def post_request(session: AuthorizedSession, url: str, data: Any) -> None:
  """Posts a request to the given url.

  Args:
    session: The authorised session.
    url: The url.
    data: The data to be posted.

  Raises:
      Error: If the request was not processed successfully.
  """
  try:
    response = session.post(url, data)
    response.raise_for_status()
  except requests.exceptions.HTTPError as error:
    logging.exception('HTTPError "%s" "%s": post_request failed',
                      error.response.status_code, error.response.reason)
    raise Error('HTTPError {} {}: post_request failed.'.format(
        error.response.status_code, error.response.reason))


def disable_api(session: AuthorizedSession, project_id: str, api: str) -> None:
  """Disables Cloud API for a given project.

  Args:
    session: The authorised session.
    project_id: GCP project id.
    api: The API to be disabled.
  """
  disable_api_url = '{}/{}/services/{}:disable'.format(_SERVICE_URL, project_id,
                                                       api)
  logging.info('Disabling following API for "%s" project : "%s".', project_id,
               api)
  post_request(session, disable_api_url, {'disableDependentServices': True})


def is_api_enabled(session: AuthorizedSession, project_id: str,
                   api: str) -> bool:
  """Checks if Cloud API is enabled for given project.

  Args:
    session: The authorised session.
    project_id: GCP project id.
    api: The API to be checked.

  Returns:
    True: If the API is enabled.
    False: If the API is not enabled.
  """
  get_service_url = '{}/{}/services/{}'.format(_SERVICE_URL, project_id, api)
  response = session.get(get_service_url)
  service = json.loads(response.content)
  if service['state'] == 'ENABLED':
    return True
  return False
