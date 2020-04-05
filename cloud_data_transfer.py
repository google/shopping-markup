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

import logging

from plugins.cloud_utils import cloud_auth
from plugins.cloud_utils import utils

_MERCHANT_CENTER_ID = 'merchant_center'  # Data source id for Merchant Center.
_GOOGLE_ADS_ID = 'adwords'  # Data source id for Google Ads.


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

  def create_merchant_center_transfer(self, merchant_id: str,
                                      destination_dataset: str) -> None:
    """Creates a new merchant center transfer.

    Merchant center allows retailers to store product info into Google. This
    method creates a data transfer config to copy the product data to BigQuery.

    Args:
      merchant_id: Google Merchant Center(GMC) account id.
      destination_dataset: BigQuery dataset id.
    """
    logging.info(
        'Creating data transfer for merchant id %s to destination dataset %s',
        merchant_id, destination_dataset)
    parent = f'projects/{self.project_id}'
    body = {
        'display_name': f'Merchant Center Transfer - {merchant_id}',
        'data_source_id': _MERCHANT_CENTER_ID,
        'destination_dataset_id': destination_dataset,
        'params': {
            'merchant_id': merchant_id,
            'export_products': True
        }
    }
    request = self.client.projects().transferConfigs().create(
        parent=parent, body=body)
    utils.execute_request(request)
    logging.info(
        'Data transfer created for merchant id %s to destination dataset %s',
        merchant_id, destination_dataset)

  def create_google_ads_transfer(self, customer_id: str,
                                 destination_dataset: str) -> None:
    """Creates a new Google Ads transfer.

    This method creates a data transfer config to copy Google Ads data to
    BigQuery dataset.

    Args:
      customer_id: Google Ads customer id.
      destination_dataset: BigQuery dataset id.
    """
    logging.info(
        'Creating data transfer for Google Ads customer id %s to destination '
        'dataset %s', customer_id, destination_dataset)
    parent = f'projects/{self.project_id}'
    body = {
        'display_name': f'Google Ads Transfer - {customer_id}',
        'data_source_id': _GOOGLE_ADS_ID,
        'destination_dataset_id': destination_dataset,
        'params': {
            'customer_id': customer_id
        }
    }
    request = self.client.projects().transferConfigs().create(
        parent=parent, body=body)
    utils.execute_request(request)
    logging.info(
        'Data transfer created for Google Ads customer id %s to destination '
        'dataset %s', customer_id, destination_dataset)
