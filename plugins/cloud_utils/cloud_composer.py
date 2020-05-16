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

"""Manage operations on Cloud Composer."""

from typing import Any, Dict
import logging

from googleapiclient import errors
from plugins.cloud_utils import cloud_auth
from plugins.cloud_utils import utils

# Default Cloud Composer configuration.
_LOCATION = 'us-central1'
_DISC_SIZE = 20  # In GBs.
_MACHINE_TYPE = 'n1-standard-1'
_PYTHON_VERSION = '3'
_HTTP_CONFLICT_CODE = 409


class Error(Exception):
  """A generic error thrown for any exception in cloud_composer module."""
  pass


class CloudComposerUtils(object):
  """CloudComposerUtils class provides methods to manage Composer environment.

  This class manages Cloud Composer service within a single GCP
  project.

  Typical usage example:
       >>> composer = CloudComposerUtils('project_id',
                                         'us-west1',
                                         'service_account_key_file.json')
       >>> composer.create_environment()
  """

  _CONFIG_KEY = 'config'  # Key to get Composer configurations.
  _DAG_FOLDER_KEY = 'dagGcsPrefix'  # Key to get Cloud Storage DAG folder URL.

  def __init__(self,
               project_id: str,
               location: str = _LOCATION,
               service_account_key_file: str = None) -> None:
    """Initialise new instance of CloudComposerUtils.

    Args:
      project_id: GCP project id.
      location: Optional. Region under which the Composer environment needs to
        be managed. It defaults to 'us-central1'. Allowed values -
        https://cloud.google.com/compute/docs/regions-zones/.
      service_account_key_file: Optional. File containing service account key.
        If not passed the default credential will be used. There are following
        ways to create service accounts:
          1. Use `build_service_client` method from `cloud_auth` module.
          2. Use `gcloud` command line utility as documented here -
             https://cloud.google.com/iam/docs/creating-managing-service-account-keys
    """
    self.client = cloud_auth.build_service_client('composer',
                                                  service_account_key_file)
    self.project_id = project_id
    self.location = location

  def _get_fully_qualified_env_name(self, environment_name: str) -> str:
    """Constructs fully qualified environment name.

    Args:
      environment_name: Name of Composer environment.

    Returns:
      fully_qualified_name: Fully qualified environment name in the following
        format -
        projects/project_id/location/location_name/environment/environment_name
    """
    fully_qualified_name = (f'projects/{self.project_id}/locations/'
                            f'{self.location}/environments/{environment_name}')
    return fully_qualified_name

  def create_environment(self,
                         environment_name: str,
                         zone: str = 'b',
                         disk_size_gb: int = _DISC_SIZE,
                         machine_type: str = _MACHINE_TYPE,
                         image_version: str = None,
                         python_version: str = _PYTHON_VERSION) -> None:
    """Creates new Cloud Composer environment.

    Args:
      environment_name: Name of Composer environment.
      zone: Optional. Zone where the Composer environment will be created. It
        defaults to 'b' since zone 'b' is present in all the regions.
        Allowed values - https://cloud.google.com/compute/docs/regions-zones/.
      disk_size_gb: Optional. The disk size in GB used for node VMs. It defaults
        to 20GB since it is the minimum size.
      machine_type: Optional. The parameter will specify what type of VM to
        create.It defaults to 'n1-standard-1'. Allowed values -
        https://cloud.google.com/compute/docs/machine-types.
      image_version: The version of Composer and Airflow running in the
        environment. If this is not provided, a default version is used as per
        https://cloud.google.com/composer/docs/concepts/versioning/composer-versions.
      python_version: The version of Python used to run the Apache Airflow. It
        defaults to '3'.

    Raises:
      Error: If the provided disk size is less than 20GB.
    """
    if disk_size_gb < 20:
      raise Error(('The minimum disk size needs to be 20GB to create Composer '
                   'environment'))
    fully_qualified_name = self._get_fully_qualified_env_name(environment_name)
    parent = f'projects/{self.project_id}/locations/{self.location}'
    composer_zone = f'{self.location}-{zone}'
    location = f'projects/{self.project_id}/zones/{composer_zone}'
    machine_type = (f'projects/{self.project_id}/zones/{composer_zone}/'
                    f'machineTypes/{machine_type}')
    software_config = {
        'pythonVersion': python_version
    }
    if image_version:
      software_config['imageVersion'] = image_version
    request_body = {
        'name': fully_qualified_name,
        'config': {
            'nodeConfig': {
                'location': location,
                'machineType': machine_type,
                'diskSizeGb': disk_size_gb
            },
            'softwareConfig': software_config
        }
    }
    logging.info('Creating "%s" Composer environment for "%s" project.',
                 fully_qualified_name, self.project_id)
    try:
      request = self.client.projects().locations().environments().create(
          parent=parent, body=request_body)
      operation = utils.execute_request(request)
      operation_client = self.client.projects().locations().operations()
      utils.wait_for_operation(operation_client, operation)
    except errors.HttpError as error:
      if error.__dict__['resp'].status == _HTTP_CONFLICT_CODE:
        logging.info('The Composer environment %s already exists.',
                     fully_qualified_name)
        return
      logging.exception('Error occurred while creating Composer environment.')
      raise Error('Error occurred while creating Composer environment.')

  def install_python_packages(self, environment_name: str,
                              packages: Dict[str, str]) -> None:
    """Install Python packages on the existing Composer environment.

    Args:
      environment_name: Name of the existing Composer environment. The fully
        qualified environment name will be constructed as follows -
        'projects/{project_id}/locations/{location}/environments/
        {environment_name}'.
      packages: Dictionary of Python packages to be installed in the Composer
        environment. Each entry in the dictionary has dependency name as the key
        and version as the value. e.g -
        {'tensorflow' : "<=1.0.1", 'apache-beam': '==2.12.0', 'flask': '>1.0.3'}

    Raises:
      Error: If the list of packages is empty.
    """
    if not packages:
      raise Error('Package list cannot be empty.')
    fully_qualified_name = self._get_fully_qualified_env_name(environment_name)
    logging.info('Installing "%s" packages in "%s" Composer environment.',
                 packages, fully_qualified_name)
    try:
      request_body = {
          'name': fully_qualified_name,
          'config': {
              'softwareConfig': {
                  'pypiPackages': packages
              }
          }
      }
      request = (
          self.client.projects().locations().environments().patch(
              name=fully_qualified_name,
              body=request_body,
              updateMask='config.softwareConfig.pypiPackages'))
      operation = utils.execute_request(request)
      operation_client = self.client.projects().locations().operations()
      utils.wait_for_operation(operation_client, operation)
      logging.info('Installed "%s" packages in "%s" Composer environment.',
                   packages, fully_qualified_name)
    except errors.HttpError:
      logging.exception('Error occurred while installing packages.')
      raise Error('Error occurred while installing python packages.')

  def set_environment_variables(self, environment_name: str,
                                environment_variables: Dict[str, str]) -> None:
    """Sets environment variables on the existing Composer environment.

    Args:
      environment_name: Name of the existing Composer environment. The fully
        qualified environment name will be constructed as follows -
        'projects/{project_id}/locations/{location}/environments/
        {environment_name}'.
      environment_variables: Environment variables to be added to the Composer
        environment.

    Raises:
      Error: If the request was not processed successfully.
    """
    fully_qualified_name = self._get_fully_qualified_env_name(environment_name)
    logging.info(
        'Setting "%s" environment variables in "%s" Composer '
        'environment.', environment_variables, fully_qualified_name)
    try:
      request_body = {
          'name': fully_qualified_name,
          'config': {
              'softwareConfig': {
                  'envVariables': environment_variables
              }
          }
      }
      request = (
          self.client.projects().locations().environments().patch(
              name=fully_qualified_name,
              body=request_body,
              updateMask='config.softwareConfig.envVariables'))
      operation = utils.execute_request(request)
      operation_client = self.client.projects().locations().operations()
      utils.wait_for_operation(operation_client, operation)
      logging.info(
          'Updated "%s" environment variables in "%s" Composer '
          'environment.', environment_variables, fully_qualified_name)
    except errors.HttpError:
      logging.exception('Error occurred while setting environment variables.')
      raise Error('Error occurred while setting environment variables.')

  def override_airflow_configs(
      self, environment_name: str, airflow_config_overrides: Dict[str,
                                                                  str]) -> None:
    """Overrides Airflow configurations on the existing Composer environment.

    Args:
      environment_name: Name of the existing Composer environment. The fully
        qualified environment name will be constructed as follows -
        'projects/{project_id}/locations/{location}/environments/
        {environment_name}'.
      airflow_config_overrides: Airflow configurations to be overridden in the
        Composer environment.

    Raises:
      Error: If the request was not processed successfully.
    """
    fully_qualified_name = self._get_fully_qualified_env_name(environment_name)
    logging.info(
        'Overriding "%s" Airflow configurations in "%s" Composer '
        'environment.', airflow_config_overrides, fully_qualified_name)
    try:
      request_body = {
          'name': fully_qualified_name,
          'config': {
              'softwareConfig': {
                  'airflowConfigOverrides': airflow_config_overrides
              }
          }
      }
      request = (
          self.client.projects().locations().environments().patch(
              name=fully_qualified_name,
              body=request_body,
              updateMask='config.softwareConfig.airflowConfigOverrides'))
      operation = utils.execute_request(request)
      operation_client = self.client.projects().locations().operations()
      utils.wait_for_operation(operation_client, operation)
      logging.info(
          'Airflow configurations "%s" has been overridden in "%s" Composer '
          'environment.', airflow_config_overrides, fully_qualified_name)
    except errors.HttpError:
      logging.exception(
          'Error occurred while overriding Airflow configurations.')
      raise Error('Error occurred while overriding Airflow configurations.')

  def get_environment(self, environment_name: str) -> Dict[str, Any]:
    """Retrieves details of a Composer environment.

    Args:
      environment_name: Name of the existing Composer environment. The fully
        qualified environment name will be constructed as follows -
        'projects/{project_id}/locations/{location}/environments/
        {environment_name}'.

    Returns:
      environment: Details of Composer environment.

    Raises:
      Error: If the request was not processed successfully.
    """
    fully_qualified_name = self._get_fully_qualified_env_name(environment_name)
    logging.info('Retrieving Composer environment details for "%s"',
                 fully_qualified_name)
    try:
      request = self.client.projects().locations().environments().get(
          name=fully_qualified_name)
      composer_environment_details = utils.execute_request(request)
      return composer_environment_details
    except errors.HttpError:
      logging.exception('Error while retrieving Composer environment details.')
      raise Error('Error while retrieving Composer environment details.')

  def get_dags_folder(self, environment_name: str) -> str:
    """Returns Cloud Storage URL for DAGs folder for a Composer environment.

    Args:
      environment_name: Name of the existing Composer environment. The fully
        qualified environment name will be constructed as follows -
        'projects/{project_id}/locations/{location}/environments/
        {environment_name}'.

    Returns:
      The Cloud Storage DAGs URL.
    """
    environment_details = self.get_environment(environment_name)
    environment_config = environment_details[self._CONFIG_KEY]
    return environment_config[self._DAG_FOLDER_KEY]
