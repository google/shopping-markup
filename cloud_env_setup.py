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

from google.cloud import bigquery
from google.cloud import exceptions
from plugins.cloud_utils import cloud_api
import cloud_data_transfer


_DEFAULT_DATASET_LOCATION = 'US'

# Set logging level.
logging.getLogger().setLevel(logging.INFO)

# Required Cloud APIs to be enabled.
_APIS_TO_BE_ENABLED = [
    'bigquery.googleapis.com', 'bigquerydatatransfer.googleapis.com'
]
_DATASET_ID = 'markup'


def enable_apis(project_id: str) -> None:
  """Enables list of cloud APIs for given cloud project.

  Args:
    project_id: A cloud project id.
  """
  cloud_api_utils = cloud_api.CloudApiUtils(project_id=project_id)
  cloud_api_utils.enable_apis(_APIS_TO_BE_ENABLED)


def create_dataset_if_not_exists(project_id: str, dataset_id: str) -> None:
  """Creates BigQuery dataset if it doesn't exists.

  Args:
    project_id: A cloud project id.
    dataset_id: BigQuery dataset id.
  """
  # Construct a BigQuery client object.
  client = bigquery.Client()
  fully_qualified_dataset_id = f'{project_id}.{dataset_id}'
  try:
    client.get_dataset(fully_qualified_dataset_id)
    logging.info('Dataset %s already exists.', fully_qualified_dataset_id)
  except exceptions.NotFound:
    logging.info('Dataset %s is not found.', fully_qualified_dataset_id)
    dataset = bigquery.Dataset(fully_qualified_dataset_id)
    dataset.location = _DEFAULT_DATASET_LOCATION
    client.create_dataset(dataset)
    logging.info('Dataset %s created.', fully_qualified_dataset_id)


def load_language_codes(project_id: str, dataset_id: str) -> None:
  """Loads language codes."""
  client = bigquery.Client(project=project_id)
  fully_qualified_table_id = f'{project_id}.{dataset_id}.language_codes'
  job_config = bigquery.LoadJobConfig(
      source_format=bigquery.SourceFormat.CSV,
      skip_leading_rows=1,
      autodetect=True,
  )
  file_name = 'data/language_codes.csv'
  with open(file_name, 'rb') as source_file:
    job = client.load_table_from_file(
        source_file, fully_qualified_table_id, job_config=job_config)

  job.result()


def load_geo_targets(project_id: str, dataset_id: str) -> None:
  """Loads geo targets."""
  client = bigquery.Client(project=project_id)
  fully_qualified_table_id = f'{project_id}.{dataset_id}.geo_targets'
  job_config = bigquery.LoadJobConfig(
      source_format=bigquery.SourceFormat.CSV,
      skip_leading_rows=1,
      autodetect=True,
  )
  file_name = 'data/geo_targets.csv'
  with open(file_name, 'rb') as source_file:
    job = client.load_table_from_file(
        source_file, fully_qualified_table_id, job_config=job_config)

  job.result()


def read_file(file_path: str) -> str:
  """Reads and returns contents of the file.

  Args:
    file_path: File path.

  Returns:
    content: File content.

  Raises:
      FileNotFoundError: If the provided file is not found.
  """
  try:
    with open(file_path, 'r') as stream:
      content = stream.read()
  except FileNotFoundError:
    raise FileNotFoundError(f'The file "{file_path}" could not be found.')
  else:
    return content


def configure_sql(sql_path: str, query_params: Dict[str, Union[str, int,
                                                               float]]) -> str:
  """Configures parameters of SQL script with variables supplied.

  Args:
    sql_path: Path to SQL script.
    query_params: Configuration containing query parameter values.

  Returns:
    sql_script: String representation of SQL script with parameters assigned.
  """
  sql_script = read_file(sql_path)

  params = {}
  for param_key, param_value in query_params.items():
    # If given value is list of strings (ex. 'a,b,c'), create tuple of
    # strings (ex. ('a', 'b', 'c')) to pass to SQL IN operator.
    if isinstance(param_value, str) and ',' in param_value:
      params[param_key] = tuple(param_value.split(','))
    else:
      params[param_key] = param_value

  return sql_script.format(**params)


def execute_queries(project_id: str, dataset_id: str, merchant_id: str,
                    customer_id: str) -> None:
  """Executes list of queries."""
  sql_files = [
      '1_product_view.sql', '2_product_metrics_view.sql', '3_customer_view.sql',
      '4_product_detailed_view.sql'
  ]
  prefix = 'scripts'
  query_params = {
      'project_id': project_id,
      'dataset': dataset_id,
      'merchant_id': merchant_id,
      'external_customer_id': customer_id
  }
  client = bigquery.Client(project=project_id)
  for sql_file in sql_files:
    query = configure_sql(os.path.join(prefix, sql_file), query_params)
    query_job = client.query(query)
    query_job.result()


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
  return parser.parse_args()


def main():
  args = parse_arguments()
  data_transfer = cloud_data_transfer.CloudDataTransferUtils(args.project_id)
  logging.info('Enabling APIs.')
  enable_apis(args.project_id)
  logging.info('Enabled APIs.')
  logging.info('Creating %s dataset.', args.dataset_id)
  create_dataset_if_not_exists(args.project_id, args.dataset_id)
  logging.info('Created %s dataset.', args.dataset_id)
  logging.info('Creating Merchant Center Transfer.')
  merchant_center_config = data_transfer.create_merchant_center_transfer(
      args.merchant_id, args.dataset_id)
  logging.info('Created Merchant Center Transfer.')
  logging.info('Creating Google Ads Transfer.')
  ads_config = data_transfer.create_google_ads_transfer(args.ads_customer_id,
                                                        args.dataset_id)
  logging.info('Created Google Ads Transfer.')
  data_transfer.wait_for_transfer_completion(merchant_center_config)
  data_transfer.wait_for_transfer_completion(ads_config)
  load_language_codes(args.project_id, args.dataset_id)
  load_geo_targets(args.project_id, args.dataset_id)
  logging.info('Creating MarkUp specific views.')
  execute_queries(args.project_id, args.dataset_id, args.merchant_id,
                  args.ads_customer_id)
  logging.info('Created MarkUp specific views.')


if __name__ == '__main__':
  main()
