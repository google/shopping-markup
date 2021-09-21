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
"""Cloud Environment setup module for MarkUp premium.

This module automates the following 4 steps:
  1. Enable all the required Cloud APIs.
  2. Create GMC and Google Ads data transfer service.
  3. Setup tables and views in BigQuery.
"""

import argparse
import logging
import os
from typing import Dict, Union

from google.cloud import exceptions
from plugins.cloud_utils import cloud_api
import cloud_bigquery
import cloud_data_transfer
import config_parser


# Set logging level.
logging.getLogger().setLevel(logging.INFO)
logging.getLogger('googleapiclient.discovery').setLevel(logging.WARNING)

# Required Cloud APIs to be enabled.
_APIS_TO_BE_ENABLED = [
    'bigquery.googleapis.com', 'bigquerydatatransfer.googleapis.com'
]
_DATASET_ID = 'markup'
_MATERIALIZE_PRODUCT_DETAILED_SQL = 'scripts/materialize_product_detailed.sql'
_MATERIALIZE_PRODUCT_HISTORICAL_SQL = 'scripts/materialize_product_historical.sql'


def enable_apis(project_id: str) -> None:
  """Enables list of cloud APIs for given cloud project.

  Args:
    project_id: A cloud project id.
  """
  cloud_api_utils = cloud_api.CloudApiUtils(project_id=project_id)
  cloud_api_utils.enable_apis(_APIS_TO_BE_ENABLED)


def parse_boolean(arg: str):
  """Returns boolean representation of argument."""
  arg = str(arg).lower()
  if 'true'.startswith(arg):
      return True
  return False


def parse_arguments() -> argparse.Namespace:
  """Initialize command line parser using argparse.

  Returns:
    An argparse.ArgumentParser.
  """
  parser = argparse.ArgumentParser()
  parser.add_argument('--project_id', help='GCP project id.', required=True)
  parser.add_argument(
      '--dataset_id',
      help='BigQuery dataset id.',
      default=_DATASET_ID,
      required=False)
  parser.add_argument(
      '--merchant_id', help='Google Merchant Center Account Id.', required=True)
  parser.add_argument(
      '--ads_customer_id',
      help='Google Ads External Customer Id.',
      required=True)
  parser.add_argument(
      '--market_insights',
      help='Deploy Market Insights solution.',
      type=parse_boolean,
      required=True)
  return parser.parse_args()


def main():
  args = parse_arguments()
  ads_customer_id = args.ads_customer_id.replace('-', '')
  data_transfer = cloud_data_transfer.CloudDataTransferUtils(args.project_id)
  logging.info('Enabling APIs.')
  enable_apis(args.project_id)
  logging.info('Enabled APIs.')
  logging.info('Creating %s dataset.', args.dataset_id)
  cloud_bigquery.create_dataset_if_not_exists(args.project_id, args.dataset_id)
  merchant_center_config = data_transfer.create_merchant_center_transfer(
      args.merchant_id, args.dataset_id, args.market_insights)
  ads_config = data_transfer.create_google_ads_transfer(ads_customer_id,
                                                        args.dataset_id)
  try:
    logging.info('Checking the GMC data transfer status.')
    data_transfer.wait_for_transfer_completion(merchant_center_config)
    logging.info('The GMC data have been successfully transferred.')
  except cloud_data_transfer.DataTransferError:
    logging.error('If you have just created GMC transfer - you may need to'
                  'wait for up to 90 minutes before the data of your Merchant'
                  'account are prepared and available for the transfer.')
    raise
  logging.info('Checking the Google Ads data transfer status.')
  data_transfer.wait_for_transfer_completion(ads_config)
  logging.info('The Google Ads data have been successfully transferred.')
  cloud_bigquery.load_language_codes(args.project_id, args.dataset_id)
  cloud_bigquery.load_geo_targets(args.project_id, args.dataset_id)
  logging.info('Creating MarkUp specific views.')
  cloud_bigquery.execute_queries(args.project_id, args.dataset_id, args.merchant_id,
                                 ads_customer_id, args.market_insights)
  logging.info('Created MarkUp specific views.')
  logging.info('Updating targeted products')
  query_params = {
      'project_id': args.project_id,
      'dataset': args.dataset_id,
      'merchant_id': args.merchant_id,
      'external_customer_id': ads_customer_id
  }
  query = cloud_bigquery.get_main_workflow_sql(
    args.project_id, args.dataset_id, args.merchant_id, ads_customer_id)
  data_transfer.schedule_query(f'Main workflow - {args.dataset_id} - {ads_customer_id}',
                               query)
  logging.info('Job created to run markup main workflow.')
  if args.market_insights:
    logging.info('Market insights requested, creating scheduled query')
    best_sellers_query = cloud_bigquery.get_best_sellers_workflow_sql(
        args.project_id, args.dataset_id, args.merchant_id)
    data_transfer.schedule_query(
        f'Best sellers workflow - {args.dataset_id} - {args.merchant_id}',best_sellers_query)
    logging.info('Job created to run best sellers workflow.')
  logging.info('MarkUp installation is complete!')


if __name__ == '__main__':
  main()
