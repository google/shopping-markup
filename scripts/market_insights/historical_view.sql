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

-- Creates a historical view for Performance, Status, Price & Price Benchmarks.
CREATE OR REPLACE VIEW `{project_id}.{dataset}.market_insights_historical_view` AS (
      SELECT
        data_date,
        unique_product_id,
        target_country,
        price,
        price_currency,
        sale_price,
        sale_price_currency,
        price_benchmark_value,
        price_benchmark_currency,
        price_benchmark_timestamp,
        CASE
          WHEN price_benchmark_value IS NULL THEN ''
          WHEN (SAFE_DIVIDE(price, price_benchmark_value) - 1) < -0.01 THEN 'Less than PB' -- ASSUMPTION: Enter % as a decimal here
          WHEN (SAFE_DIVIDE(price, price_benchmark_value) - 1) > 0.01 THEN 'More than PB' -- ASSUMPTION: Enter % as a decimal here
          ELSE 'Equal to PB'
        END AS price_competitiveness_band,
        SAFE_DIVIDE(price, price_benchmark_value) - 1 AS price_vs_benchmark,
        SAFE_DIVIDE(price, price_benchmark_value) - 1 AS sale_price_vs_benchmark,
      FROM (
        SELECT DISTINCT
          _PARTITIONDATE as data_date,
          CONCAT(CAST(merchant_id AS STRING), '|', product_id) AS unique_product_id,
          target_country,
          price.value AS price,
          price.currency as price_currency,
          sale_price.value AS sale_price,
          sale_price.currency AS sale_price_currency,
        FROM `{project_id}.{dataset}.Products_{merchant_id}` AS Products,
          Products.destinations,
          UNNEST(ARRAY_CONCAT(destinations.approved_countries, destinations.pending_countries, destinations.disapproved_countries)) AS target_country
      )
      LEFT JOIN (
        SELECT
          _PARTITIONDATE as data_date,
          CONCAT(CAST(merchant_id AS STRING), '|', product_id) AS unique_product_id,
          country_of_sale as target_country,
          price_benchmark_value,
          price_benchmark_currency,
          price_benchmark_timestamp
        FROM `{project_id}.{dataset}.Products_PriceBenchmarks_{merchant_id}`
      )
      USING (data_date, unique_product_id, target_country)
)
