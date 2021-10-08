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

-- DDL definition for TargetedProduct table.
--
-- Using `CREATE IF NOT EXISTS` as the table is used in `product_detailed` views.
CREATE OR REPLACE TABLE `{project_id}.{dataset}.TargetedProduct_{external_customer_id}`
(
  data_date DATE,
  product_id STRING,
  merchant_id INT64,
  target_country STRING
);

-- DDL definition for ParsedCriteria table.
CREATE OR REPLACE TABLE `{project_id}.{dataset}.ParsedCriteria_{external_customer_id}`
(
  criteria STRING,
  custom_label0 STRING,
  custom_label1 STRING,
  custom_label2 STRING,
  custom_label3 STRING,
  custom_label4 STRING,
  product_type_l1 STRING,
  product_type_l2 STRING,
  product_type_l3 STRING,
  product_type_l4 STRING,
  product_type_l5 STRING,
  google_product_category_l1 STRING,
  google_product_category_l2 STRING,
  google_product_category_l3 STRING,
  google_product_category_l4 STRING,
  google_product_category_l5 STRING,
  brand STRING,
  offer_id STRING,
  channel STRING,
  channel_exclusivity STRING,
  condition STRING
);
