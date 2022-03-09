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
        pb.unique_product_id,
        pb.target_country,
        pb.price_benchmark_value,
        pb.price_benchmark_currency,
        pb.price_benchmark_timestamp,
        CASE
          WHEN pb.price_benchmark_value IS NULL THEN ''
          WHEN (SAFE_DIVIDE(product.effective_price_value, pb.price_benchmark_value) - 1) < -0.01
            THEN 'Less than PB'  -- ASSUMPTION: Enter % as a decimal here
          WHEN (SAFE_DIVIDE(product.effective_price_value, pb.price_benchmark_value) - 1) > 0.01
            THEN 'More than PB'  -- ASSUMPTION: Enter % as a decimal here
          ELSE 'Equal to PB'
          END AS price_competitiveness_band,
        SAFE_DIVIDE(product.effective_price_value, pb.price_benchmark_value) - 1 AS price_vs_benchmark,
        product.effective_price_value AS effective_price,
      FROM (
        SELECT
          unique_product_id,
          target_country,
          latest_date,
          IF(
            sale_price_effective_start_date <= CURRENT_TIMESTAMP()
              AND sale_price_effective_end_date > CURRENT_TIMESTAMP(),
            sale_price.value,
            price.value) AS effective_price_value
        FROM
          `{project_id}.{dataset}.product_detailed_materialized`
      ) AS product
      INNER JOIN (
        SELECT
          _PARTITIONDATE as data_date,
          CONCAT(CAST(merchant_id AS STRING), '|', product_id) AS unique_product_id,
          country_of_sale as target_country,
          price_benchmark_value,
          price_benchmark_currency,
          price_benchmark_timestamp
        FROM `{project_id}.{dataset}.Products_PriceBenchmarks_{merchant_id}`
      ) pb
      ON
        product.unique_product_id = pb.unique_product_id
        AND product.target_country = pb.target_country
        AND product.latest_date = pb.data_date
    ),
    best_sellers AS (
      SELECT DISTINCT
        _PARTITIONDATE AS data_date,
        CONCAT(CAST(merchant_id AS STRING), '|', product_id) AS unique_product_id,
        SPLIT(rank_id, ':')[SAFE_ORDINAL(2)] AS target_country,
        TRUE as is_best_seller,
      FROM
        `{project_id}.{dataset}.BestSellers_TopProducts_Inventory_{merchant_id}`
      WHERE _PARTITIONDATE = DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
    )
  SELECT
    product,
    price_benchmarks,
    best_sellers,
  FROM (
    SELECT 
      *, 
      IF(
       sale_price_effective_start_date <= CURRENT_TIMESTAMP()
         AND sale_price_effective_end_date > CURRENT_TIMESTAMP(),
       sale_price.value,
       price.value) AS effective_price,
    FROM `{project_id}.{dataset}.product_detailed_materialized`
  ) AS product
  LEFT JOIN price_benchmarks
    ON product.unique_product_id = price_benchmarks.unique_product_id
    AND product.target_country = price_benchmarks.target_country
    AND product.latest_date = price_benchmarks.data_date
  LEFT JOIN best_sellers
    ON product.unique_product_id = best_sellers.unique_product_id
    AND product.target_country = best_sellers.target_country
    AND product.latest_date = best_sellers.data_date
);
