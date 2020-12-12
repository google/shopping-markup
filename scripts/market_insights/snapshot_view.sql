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

-- Creates a latest snapshot view with Best Sellers & Price Benchmarks
CREATE OR REPLACE VIEW `{project_id}.{dataset}.market_insights_snapshot_view` AS (
  WITH
    price_benchmarks AS (
      SELECT
        pb.data_date AS data_date,
        unique_product_id,
        price_benchmark_value,
        price_benchmark_currency,
        price_benchmark_timestamp,
        CASE
          WHEN price_benchmark_value IS NULL THEN ''
          WHEN (SAFE_DIVIDE(product.price.value, price_benchmark_value) - 1) < -0.01 THEN 'Less than PB' -- ASSUMPTION: Enter % as a decimal here
          WHEN (SAFE_DIVIDE(product.price.value, price_benchmark_value) - 1) > 0.01 THEN 'More than PB' -- ASSUMPTION: Enter % as a decimal here
          ELSE 'Equal to PB'
        END AS price_competitiveness_band,
        SAFE_DIVIDE(product.price.value, price_benchmark_value) - 1 AS price_vs_benchmark,
        SAFE_DIVIDE(product.price.value, price_benchmark_value) - 1 AS sale_price_vs_benchmark,
      FROM `{project_id}.{dataset}.product_detailed_materialized` product
      JOIN (
        SELECT
          _PARTITIONDATE as data_date,
          CONCAT(CAST(merchant_id AS STRING), '|', product_id) AS unique_product_id,
          country_of_sale as target_country,
          price_benchmark_value,
          price_benchmark_currency,
          price_benchmark_timestamp
        FROM `{project_id}.{dataset}.Products_PriceBenchmarks_{merchant_id}`
        WHERE _PARTITIONDATE IN (
          (
            SELECT MAX(_PARTITIONDATE)
            FROM
              `{project_id}.{dataset}.Products_PriceBenchmarks_{merchant_id}`
          )
        )
      ) pb
      USING (unique_product_id, target_country)
    ),
    best_sellers AS (
      SELECT DISTINCT
        _PARTITIONDATE AS data_date,
        CONCAT(CAST(merchant_id AS STRING), '|', product_id) AS unique_product_id,
        TRUE as is_best_seller,
      FROM
        `{project_id}.{dataset}.BestSellers_TopProducts_Inventory_{merchant_id}`
      WHERE
        _PARTITIONDATE IN (
          (
            SELECT MAX(_PARTITIONDATE)
            FROM
              `{project_id}.{dataset}.BestSellers_TopProducts_Inventory_{merchant_id}`
          )
        )
        -- Filters for best seller status when target country = rank country
        AND SPLIT(product_id, ':')[SAFE_ORDINAL(3)] = SPLIT(rank_id, ':')[SAFE_ORDINAL(2)]
    )
  SELECT
    product,
    price_benchmarks,
    best_sellers,
  FROM `{project_id}.{dataset}.product_detailed_materialized` product
  LEFT JOIN price_benchmarks USING (unique_product_id)
  LEFT JOIN best_sellers USING (unique_product_id)
);
