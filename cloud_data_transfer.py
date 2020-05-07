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

# python3
# TODO: Move the module to cloud_utils after third_party migration.
"""Module for managing BigQuery data transfers."""

import datetime
import logging
import time
from typing import Any, Dict, List

import pytz

import auth
from plugins.cloud_utils import cloud_auth
from plugins.cloud_utils import utils

_MERCHANT_CENTER_ID = 'merchant_center'  # Data source id for Merchant Center.
_GOOGLE_ADS_ID = 'adwords'  # Data source id for Google Ads.
_SLEEP_SECONDS = 10  # Seconds to sleep before checking resource status.
_MAX_POLL_COUNTER = 200
_LOCATION = 'us'  # The only location available today for BQ Data Transfer.


class Error(Exception):
  """Base error for this module."""


class DataTransferError(Error):
  """An exception to be raised when data transfer was not successful."""


class CloudDataTransferUtils(object):
  """This class provides methods to manage BigQuery data transfers.

  Typical usage example:
    >>> data_transfer = CloudDataTransferUtils('project_id')
    >>> data_transfer.create_merchant_center_transfer(12345, 'dataset_id')
  """

  def __init__(self, project_id: str):
    """Initialise new instance of CloudDataTransferUtils.

    Args:
      project_id: GCP project id.
    """
    self.project_id = project_id
    self.client = cloud_auth.build_service_client('bigquerydatatransfer')

  def wait_for_transfer_completion(self, transfer_config: Dict[str,
                                                               Any]) -> None:
    """Waits for the completion of data transfer operation.

    This method retrieves data transfer operation and checks for its status. If
    the operation is not completed, then the operation is re-checked after
    `_SLEEP_SECONDS` seconds.

    Args:
      transfer_config: Resource representing data transfer.

    Raises:
      DataTransferError: If the data transfer is not successfully completed.
    """
    # TODO: Use exponential back-off for polling.
    transfer_config_name = transfer_config['name']
    transfer_config_id = transfer_config_name.split('/')[-1]
    parent = f'projects/{self.project_id}/transferConfigs/{transfer_config_id}'
    poll_counter = 0  # Counter to keep polling count.
    while True:
      request = self.client.projects().transferConfigs().runs().list(
          parent=parent)
      response = utils.execute_request(request)
      latest_transfer = response['transferRuns'][0]
      if latest_transfer['state'] == 'SUCCEEDED':
        logging.info('Transfer %s was successful.', transfer_config_name)
        return
      if (latest_transfer['state'] == 'FAILED' or
          latest_transfer['state'] == 'CANCELLED'):
        error_message = ('Transfer %s was not successful. Error - %s.',
                         transfer_config_name,
                         latest_transfer['errorStatus']['message'])
        logging.error(error_message)
        raise DataTransferError(error_message)
      logging.info(
          'Transfer %s still in progress. Sleeping for %s seconds before '
          'checking again.', transfer_config_name, _SLEEP_SECONDS)
      time.sleep(_SLEEP_SECONDS)
      poll_counter += 1
      if poll_counter >= _MAX_POLL_COUNTER:
        error_message = (f'Transfer {transfer_config_name} is taking too long '
                         'to finish. Hence failing the request.')
        logging.error(error_message)
        raise DataTransferError(error_message)

  def create_merchant_center_transfer(
      self, merchant_id: str, destination_dataset: str) -> Dict[str, Any]:
    """Creates a new merchant center transfer.

    Merchant center allows retailers to store product info into Google. This
    method creates a data transfer config to copy the product data to BigQuery.

    Args:
      merchant_id: Google Merchant Center(GMC) account id.
      destination_dataset: BigQuery dataset id.

    Returns:
      Newly created transfer config.
    """
    logging.info(
        'Creating data transfer for merchant id %s to destination dataset %s',
        merchant_id, destination_dataset)
    has_valid_credentials = self._check_valid_credentials(_MERCHANT_CENTER_ID)
    authorization_code = None
    if not has_valid_credentials:
      authorization_code = self._get_authorization_code(_MERCHANT_CENTER_ID)
    parent = f'projects/{self.project_id}'
    body = {
        'display_name': f'Merchant Center Transfer - {merchant_id}',
        'data_source_id': _MERCHANT_CENTER_ID,
        'destination_dataset_id': destination_dataset,
        'params': {
            'merchant_id': merchant_id,
            'export_products': True,
            # 'export_price_benchmarks': True
        }
    }
    request = self.client.projects().transferConfigs().create(
        parent=parent, authorizationCode=authorization_code, body=body)
    transfer_config = utils.execute_request(request)
    # self._wait_for_transfer_completion(transfer_config)
    logging.info(
        'Data transfer created for merchant id %s to destination dataset %s',
        merchant_id, destination_dataset)
    return transfer_config

  def create_google_ads_transfer(self,
                                 customer_id: str,
                                 destination_dataset: str,
                                 backfill_days: int = 30) -> Dict[str, Any]:
    """Creates a new Google Ads transfer.

    This method creates a data transfer config to copy Google Ads data to
    BigQuery dataset.

    Args:
      customer_id: Google Ads customer id.
      destination_dataset: BigQuery dataset id.
      backfill_days: Number of days to backfill.

    Returns:
      Newly created transfer config.
    """
    logging.info(
        'Creating data transfer for Google Ads customer id %s to destination '
        'dataset %s', customer_id, destination_dataset)
    has_valid_credentials = self._check_valid_credentials(_GOOGLE_ADS_ID)
    authorization_code = None
    if not has_valid_credentials:
      authorization_code = self._get_authorization_code(_GOOGLE_ADS_ID)
    parent = f'projects/{self.project_id}'
    body = {
        'display_name': f'Google Ads Transfer - {customer_id}',
        'data_source_id': _GOOGLE_ADS_ID,
        'destination_dataset_id': destination_dataset,
        'params': {
            'customer_id': customer_id
        },
        'dataRefreshWindowDays': backfill_days,
    }
    request = self.client.projects().transferConfigs().create(
        parent=parent, authorizationCode=authorization_code, body=body)
    transfer_config = utils.execute_request(request)
    # self._wait_for_transfer_completion(transfer_config)
    logging.info(
        'Data transfer created for Google Ads customer id %s to destination '
        'dataset %s', customer_id, destination_dataset)
    # if backfill_days:
    #   transfer_config_name = transfer_config['name']
    #   transfer_config_id = transfer_config_name.split('/')[-1]
    #   start_time = datetime.datetime.now(tz=pytz.utc) - datetime.timedelta(
    #       days=backfill_days)
    #   end_time = datetime.datetime.now(tz=pytz.utc) - datetime.timedelta(days=1)
    #   self.client.projects().transferConfigs().startManualRuns(
    #       parent=f'{parent}/transferConfigs/{transfer_config_id}',
    #       body={
    #           'requestedTimeRange': {
    #               'startTime': start_time.isoformat(),
    #               'endTime': end_time.isoformat()
    #           }
    #       }).execute()
    return transfer_config

  def _get_data_source(self, data_source_id: str) -> Dict[str, Any]:
    """Returns data source.

    Args:
      data_source_id: Data source id.
    """
    parent = f'projects/{self.project_id}'
    response = self.client.projects().dataSources().list(
        parent=parent).execute()
    for data_source in response['dataSources']:
      if data_source_id == data_source['dataSourceId']:
        return data_source

  def _check_valid_credentials(self, data_source_id: str) -> bool:
    """Returns true if valid credentials exist for the given data source.

    Args:
      data_source_id: Data source id.
    """
    name = (f'projects/{self.project_id}/locations/{_LOCATION}/dataSources/'
            f'{data_source_id}')
    response = self.client.projects().locations().dataSources().checkValidCreds(
        name=name).execute()
    if response and response['hasValidCreds']:
      return True
    return False

  def _get_authorization_code(self, data_source_id: str) -> str:
    """Returns authorization code for a given data source.

    Args:
      data_source_id: Data source id.
    """
    data_source = self._get_data_source(data_source_id)
    client_id = data_source['clientId']
    scopes = data_source['scopes']

    if not data_source:
      raise AssertionError('Invalid data source')
    return auth.retrieve_authorization_code(client_id, scopes, data_source_id)
