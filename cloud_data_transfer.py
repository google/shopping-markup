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
from typing import Any, Dict

import pytz

from google.cloud import bigquery_datatransfer_v1
from google.cloud.bigquery_datatransfer_v1 import types
from google.protobuf import struct_pb2
from google.protobuf import timestamp_pb2
import auth

_MERCHANT_CENTER_ID = 'merchant_center'  # Data source id for Merchant Center.
_GOOGLE_ADS_ID = 'adwords'  # Data source id for Google Ads.
_SLEEP_SECONDS = 10  # Seconds to sleep before checking resource status.
_MAX_POLL_COUNTER = 200
_LOCATION = 'us'  # The only location available today for BQ Data Transfer.
_SUCCESS_STATE = 3
_FAILED_STATE = 4
_CANCELLED_STATE = 5


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
    self.client = bigquery_datatransfer_v1.DataTransferServiceClient()

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
    transfer_config_name = transfer_config.name
    transfer_config_id = transfer_config_name.split('/')[-1]
    poll_counter = 0  # Counter to keep polling count.
    while True:
      transfer_config_path = self.client.location_transfer_config_path(
          self.project_id, _LOCATION, transfer_config_id)
      response = self.client.list_transfer_runs(transfer_config_path)
      latest_transfer = None
      for transfer in response:
        latest_transfer = transfer
        break
      if not latest_transfer:
        return
      if latest_transfer.state == _SUCCESS_STATE:
        logging.info('Transfer %s was successful.', transfer_config_name)
        return
      if (latest_transfer.state == _FAILED_STATE or
          latest_transfer.state == _CANCELLED_STATE):
        error_message = (f'Transfer {transfer_config_name} was not successful. '
                         f'Error - {latest_transfer.error_status.}')
        logging.error(error_message)
        raise DataTransferError(error_message)
      logging.info(
          'Transfer %s still in progress. Sleeping for %s seconds before '
          'checking again.', transfer_config_name, _SLEEP_SECONDS)
      time.sleep(_SLEEP_SECONDS)
      poll_counter += 1
      if poll_counter >= _MAX_POLL_COUNTER:
        error_message = (f'Transfer {transfer_config_name} is taking too long'
                         ' to finish. Hence failing the request.')
        logging.error(error_message)
        raise DataTransferError(error_message)

  def _get_existing_transfer(self, data_source_id: str,
                             destination_dataset_id: str,
                             params: Dict[str, str]) -> bool:
    """Gets data transfer if it already exists.

    Args:
      data_source_id: Data source id.
      destination_dataset_id: BigQuery dataset id.
      params: Data transfer specific parameters.

    Returns:
      Data Transfer if the transfer already exists.
      None otherwise.
    """
    parent = self.client.location_path(self.project_id, _LOCATION)
    for transfer_config in self.client.list_transfer_configs(parent):
      if transfer_config.data_source_id != data_source_id:
        continue
      if transfer_config.destination_dataset_id != destination_dataset_id:
        continue
      has_param_configs = True
      for key, value in params.items():
        if transfer_config.params[key] != value:
          has_param_configs = False
          break
      if has_param_configs and transfer_config.state == _SUCCESS_STATE:
        return transfer_config
    return None

  def create_merchant_center_transfer(
      self, merchant_id: str, destination_dataset: str) -> types.TransferConfig:
    """Creates a new merchant center transfer.

    Merchant center allows retailers to store product info into Google. This
    method creates a data transfer config to copy the product data to BigQuery.

    Args:
      merchant_id: Google Merchant Center(GMC) account id.
      destination_dataset: BigQuery dataset id.

    Returns:
      Transfer config.
    """
    logging.info('Creating Merchant Center Transfer.')
    parameters = struct_pb2.Struct()
    parameters['merchant_id'] = merchant_id
    parameters['export_products'] = True
    data_transfer_config = self._get_existing_transfer(_MERCHANT_CENTER_ID,
                                                       destination_dataset,
                                                       parameters)
    if data_transfer_config:
      logging.info(
          'Data transfer for merchant id %s to destination dataset %s '
          'already exists.', merchant_id, destination_dataset)
      return data_transfer_config
    logging.info(
        'Creating data transfer for merchant id %s to destination dataset %s',
        merchant_id, destination_dataset)
    has_valid_credentials = self._check_valid_credentials(_MERCHANT_CENTER_ID)
    authorization_code = None
    if not has_valid_credentials:
      authorization_code = self._get_authorization_code(_MERCHANT_CENTER_ID)
    parent = self.client.location_path(self.project_id, _LOCATION)
    transfer_config_input = {
        'display_name': f'Merchant Center Transfer - {merchant_id}',
        'data_source_id': _MERCHANT_CENTER_ID,
        'destination_dataset_id': destination_dataset,
        'params': parameters
    }
    transfer_config = self.client.create_transfer_config(
        parent, transfer_config_input, authorization_code)
    logging.info(
        'Data transfer created for merchant id %s to destination dataset %s',
        merchant_id, destination_dataset)
    return transfer_config

  def create_google_ads_transfer(
      self,
      customer_id: str,
      destination_dataset: str,
      backfill_days: int = 30) -> types.TransferConfig:
    """Creates a new Google Ads transfer.

    This method creates a data transfer config to copy Google Ads data to
    BigQuery dataset.

    Args:
      customer_id: Google Ads customer id.
      destination_dataset: BigQuery dataset id.
      backfill_days: Number of days to backfill.

    Returns:
      Transfer config.
    """
    logging.info('Creating Google Ads Transfer.')

    customer_id = customer_id.replace('-', '')
    parameters = struct_pb2.Struct()
    parameters['customer_id'] = customer_id
    data_transfer_config = self._get_existing_transfer(_GOOGLE_ADS_ID,
                                                       destination_dataset,
                                                       parameters)
    if data_transfer_config:
      logging.info(
          'Data transfer for Google Ads customer id %s to destination dataset '
          '%s already exists.', customer_id, destination_dataset)
      return data_transfer_config
    logging.info(
        'Creating data transfer for Google Ads customer id %s to destination '
        'dataset %s', customer_id, destination_dataset)
    has_valid_credentials = self._check_valid_credentials(_GOOGLE_ADS_ID)
    authorization_code = None
    if not has_valid_credentials:
      authorization_code = self._get_authorization_code(_GOOGLE_ADS_ID)
    parent = self.client.location_path(self.project_id, _LOCATION)
    transfer_config_input = {
        'display_name': f'Google Ads Transfer - {customer_id}',
        'data_source_id': _GOOGLE_ADS_ID,
        'destination_dataset_id': destination_dataset,
        'params': parameters,
        'data_refresh_window_days': 1,
    }
    transfer_config = self.client.create_transfer_config(
        parent, transfer_config_input, authorization_code)
    logging.info(
        'Data transfer created for Google Ads customer id %s to destination '
        'dataset %s', customer_id, destination_dataset)
    if backfill_days:
      transfer_config_name = transfer_config.name
      transfer_config_id = transfer_config_name.split('/')[-1]
      start_time = datetime.datetime.now(tz=pytz.utc) - datetime.timedelta(
          days=backfill_days)
      end_time = datetime.datetime.now(tz=pytz.utc) - datetime.timedelta(days=1)
      start_time = start_time.replace(hour=0, minute=0, second=0, microsecond=0)
      end_time = end_time.replace(hour=0, minute=0, second=0, microsecond=0)
      parent = self.client.location_transfer_config_path(
          self.project_id, _LOCATION, transfer_config_id)
      start_time_pb = timestamp_pb2.Timestamp()
      end_time_pb = timestamp_pb2.Timestamp()
      start_time_pb.FromDatetime(start_time)
      end_time_pb.FromDatetime(end_time)
      self.client.schedule_transfer_runs(parent, start_time_pb, end_time_pb)
    return transfer_config

  def _get_data_source(self, data_source_id: str) -> types.DataSource:
    """Returns data source.

    Args:
      data_source_id: Data source id.
    """
    return self.client.get_data_source(data_source_id)

  def _check_valid_credentials(self, data_source_id: str) -> bool:
    """Returns true if valid credentials exist for the given data source.

    Args:
      data_source_id: Data source id.
    """
    name = self.client.location_data_source_path(self.project_id, _LOCATION,
                                                 data_source_id)
    response = self.client.check_valid_creds(name)
    return response.has_valid_creds

  def _get_authorization_code(self, data_source_id: str) -> str:
    """Returns authorization code for a given data source.

    Args:
      data_source_id: Data source id.
    """
    data_source = self._get_data_source(data_source_id)
    client_id = data_source.client_id
    scopes = data_source.scopes

    if not data_source:
      raise AssertionError('Invalid data source')
    return auth.retrieve_authorization_code(client_id, scopes, data_source_id)
