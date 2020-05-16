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

"""Perform auth related operations in GCP."""

import base64
import json
import os
import textwrap
from typing import Any, Dict
import logging

import apiclient
from googleapiclient import discovery
from googleapiclient import errors
import google.auth
from google.auth import credentials
from google.auth.transport import requests
from google.oauth2 import service_account
from plugins.cloud_utils import utils

# HTTP status code
_NOT_FOUND_ERROR_CODE = 404

# Scope to manage service accounts
_SCOPE = 'https://www.googleapis.com/auth/cloud-platform'


class Error(Exception):
  """A generic error thrown for any exceptions in cloud_auth module."""
  pass


def get_credentials(
    service_account_key_file: str = None) -> credentials.Credentials:
  """Get credentials to authenticate while calling GCP APIs.

  Args:
    service_account_key_file: Optional. File containing service account key. If
      not passed the default credential will be used.

  Returns:
    credential: Credential object to authenticate while calling GCP APIs.

  Raises:
    FileNotFoundError: If the provided file is not found.
    Error: If no default credentials are found and service account key file is
      not given.
  """
  if service_account_key_file is None:
    try:
      default_credentials, _ = google.auth.default()
      return default_credentials
    except google.auth.exceptions.DefaultCredentialsError as error:
      error_message = textwrap.dedent("""
        No default credentials found. Please run
        `gcloud auth application-default login` before continuing.
        Error: {}""".format(error))
      raise Error(error_message)
  if not os.path.isfile(service_account_key_file):
    raise FileNotFoundError(
        'The service account private key file "{}" not found.'.format(
            service_account_key_file))
  return service_account.Credentials.from_service_account_file(
      service_account_key_file, scopes=[_SCOPE])


def build_service_client(
    service_name: str,
    service_account_key_file: str = None) -> apiclient.discovery.Resource:
  """Construct a Resource for interacting with GCP service APIs.

  Args:
    service_name: Name of the service for which the client is created.
    service_account_key_file: Optional. File containing service account key. If
      not passed the default credential will be used.

  Returns:
    client: A client with methods for interacting with the service APIs.
  """
  credentials_info = get_credentials(service_account_key_file)
  return discovery.build(
      service_name, 'v1', credentials=credentials_info, cache_discovery=False)


def _get_resource_manager_client() -> apiclient.discovery.Resource:
  """Creates a new resource manager client to manage GCP projects.

  Returns:
    client: The newly created resource manager client.
  """
  return build_service_client('cloudresourcemanager')


def _get_iam_client() -> apiclient.discovery.Resource:
  """Creates a new iam client.

  Returns:
    client: The newly created client.
  """
  return build_service_client('iam')


def _get_service_account_client() -> apiclient.discovery.Resource:
  """Creates a new client to manage service accounts.

  Returns:
    client: The newly created service account client.
  """
  return _get_iam_client().projects().serviceAccounts()


def _get_service_account_email(project_id: str,
                               service_account_name: str) -> str:
  """Retrieves the service account email id.

  Args:
    project_id: GCP project id.
    service_account_name: The service account name.

  Returns:
    service_account_email: The service account email id.
  """
  service_account_email = '{}@{}.iam.gserviceaccount.com'.format(
      service_account_name, project_id)
  return service_account_email


def _get_service_account_name(project_id: str,
                              service_account_name: str) -> str:
  """Retrieves fully qualified service account name.

  Args:
    project_id: GCP project id.
    service_account_name: The service account name.

  Returns:
    service_account_name: The fully qualified service account name.
  """
  service_account_email = _get_service_account_email(project_id,
                                                     service_account_name)
  service_account_name = 'projects/{}/serviceAccounts/{}'.format(
      project_id, service_account_email)
  return service_account_name


def create_service_account(project_id: str, service_account_name: str,
                           role_name: str, file_name: str) -> Dict[str, Any]:
  """Create a new service account.

  Args:
    project_id: GCP project id.
    service_account_name: The service account name.
    role_name: The role to be assigned to the service account.
    file_name: The file where service account key will be stored.

  Returns:
    service_account: The newly created service account.

  Raises:
      ValueError: If the service_account_name is empty.
      ValueError: If the file_name is empty.
  """
  if not service_account_name:
    raise ValueError('Service account name cannot be empty.')
  if not file_name:
    raise ValueError('The file name cannot be empty.')
  service_account_details = get_service_account(project_id,
                                                service_account_name)
  if service_account_details:
    return service_account_details
  logging.info('Creating "%s" service account in "%s" project',
               service_account_name, project_id)
  request = _get_service_account_client().create(
      name='projects/' + project_id,
      body={
          'accountId': service_account_name,
          'serviceAccount': {
              'displayName': service_account_name.upper()
          },
      })

  service_account_details = utils.execute_request(request)
  set_service_account_role(project_id, service_account_name, role_name)
  create_service_account_key(project_id, service_account_name, file_name)
  return service_account_details


def get_service_account(project_id: str,
                        service_account_name: str) -> Dict[str, Any]:
  """Find the service account with given name.

  Args:
    project_id: GCP project id.
    service_account_name: The service account name.

  Returns:
    service_account: If the service account is found in the cloud project.
    None: If no service account is found.
  """
  try:
    logging.info('Retrieving "%s" service account in "%s" project',
                 service_account_name, project_id)
    name = 'projects/{}/serviceAccounts/{}@{}.iam.gserviceaccount.com'.format(
        project_id, service_account_name, project_id)
    service_account_details = _get_service_account_client().get(
        name=name).execute()
    return service_account_details
  except errors.HttpError as error:
    if error.resp.status == _NOT_FOUND_ERROR_CODE:
      return None
    logging.exception('Error occurred while retrieving service account: "%s".',
                      error)
    raise Error('Error occurred while retrieving service account.')


def create_service_account_key(project_id: str, service_account_name: str,
                               file_name: str) -> None:
  """Creates key for service account and writes the private key to the file.

  Args:
    project_id: GCP project id.
    service_account_name: The service account name.
    file_name: The file to which the private key will be written.
  """
  with open(file_name, 'w+') as file_object:
    _create_service_account_key(project_id, service_account_name, file_object)


def _create_service_account_key(project_id: str, service_account_name: str,
                                file_object: Any) -> None:
  """Creates key for service account and writes private key to the file object.

  Args:
    project_id: GCP project id.
    service_account_name: The service account name.
    file_object: The file object to which the private key will be written.
  """
  name = 'projects/{}/serviceAccounts/{}@{}.iam.gserviceaccount.com'.format(
      project_id, service_account_name, project_id)
  logging.info(
      'Creating service account key for "%s" service account in "%s" project',
      service_account_name, project_id)
  request = _get_service_account_client().keys().create(name=name, body={})
  service_account_key = utils.execute_request(request)
  private_key = base64.b64decode(service_account_key['privateKeyData']).decode()
  private_key_json = json.loads(private_key)
  json.dump(private_key_json, file_object)


def set_service_account_role(project_id, service_account_name,
                             role_name) -> None:
  """Adds role to a given service account.

  The roles grant service accounts appropriate permissions to use specific
  resource. The role_name can be either primitive, predefined or custom roles.
  Please see https://cloud.google.com/iam/docs/understanding-roles for list of
  allowed primitive and predefined roles.

  Args:
    project_id: GCP project id.
    service_account_name: The service account name.
    role_name: The role to be added to the service account. The role_name
      doesn't need "roles/" prefix to be added. Allowed values -
      https://cloud.google.com/iam/docs/understanding-roles. e.g - editor
  """
  logging.info('Adding "%s" role to "%s" service account in "%s" project',
               role_name, service_account_name, project_id)
  # Read existing binding.
  service_account_email = _get_service_account_email(project_id,
                                                     service_account_name)
  member = 'serviceAccount:{}'.format(service_account_email)
  binding = {'role': f'roles/{role_name}', 'members': [member]}
  request = _get_resource_manager_client().projects().getIamPolicy(
      resource=project_id)
  policy = utils.execute_request(request)

  # Modify binding.
  policy['bindings'].append(binding)

  # Write binding.
  set_iam_policy_request_body = {'policy': policy}
  request = _get_resource_manager_client().projects().setIamPolicy(
      resource=project_id, body=set_iam_policy_request_body)
  utils.execute_request(request)


def get_auth_session(
    service_account_key_file: str) -> requests.AuthorizedSession:
  """Creates AuthorizedSession for given service account.

  Args:
      service_account_key_file: File which contains service account private key.

  Returns:
    authorized_session: AuthorizedSession for service account.

  Raises:
    FileNotFoundError: If the provided file is not found.
  """
  credentials_info = get_credentials(service_account_key_file)
  return requests.AuthorizedSession(credentials_info)
