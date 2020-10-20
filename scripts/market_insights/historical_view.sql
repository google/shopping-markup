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
CREATE OR REPLACE VIEW `{project_id}.{dataset}.market_insights_historical` AS (
  WITH
    CountryTable AS (
      SELECT DISTINCT
        parent_id AS country_criterion,
        country_code
      FROM
        `{project_id}.{dataset}.geo_targets`
    ),
    LanguageTable AS (
      SELECT DISTINCT
        criterion_id AS language_criterion,
        language_code
      FROM
        `{project_id}.{dataset}.language_codes`
    ),
    HistoricalProductMetrics AS (
      SELECT
        _DATA_DATE as data_date,
        CONCAT(
          CAST(merchantid AS STRING),
          '|',
          LOWER(channel),
          ':',
          LanguageTable.language_code,
          ':',
          CountryTable.country_code,
          ':',
          offerid) AS unique_product_id,
        SUM(impressions) AS impressions,
        SUM(clicks) AS clicks,
        SUM(cost) AS cost,
        SUM(conversions) AS conversions,
        SUM(ConversionValue) AS conversions_value,
      FROM
        `{project_id}.{dataset}.ShoppingProductStats_{external_customer_id}`AS ShoppingProductStats
      INNER JOIN
        CountryTable
        ON CountryTable.country_criterion = ShoppingProductStats.countrycriteriaid
      INNER JOIN
        LanguageTable
        ON CAST(LanguageTable.language_criterion AS STRING) = ShoppingProductStats.languagecriteriaid
      GROUP BY 1,2
    ),
    HistoricalProductStatus AS (
      SELECT
        data_date,
        CONCAT(CAST(merchant_id AS STRING), '|', product_id) AS unique_product_id,
        MAX(CAST(IF(LOWER(servability) = 'disapproved', TRUE, FALSE) as INT64)) AS has_disapproval_issues,
        MAX(CAST(IF(LOWER(servability) = 'demoted', TRUE, FALSE) as INT64)) AS has_demotion_issues
      FROM (
        SELECT
          _PARTITIONDATE as data_date,
          merchant_id,
          product_id,
          servability,
          short_description,
        FROM `{project_id}.{dataset}.Products_{merchant_id}` products
        JOIN products.issues
      )
      GROUP BY 1,2
    ),
    HistoricalPriceBenchmarks AS (
      SELECT
        _PARTITIONDATE as data_date,
        CONCAT(CAST(merchant_id AS STRING), '|', product_id) AS unique_product_id,
        country_of_sale as target_country,
        price_benchmark_value,
        price_benchmark_currency,
        price_benchmark_timestamp
      FROM `{project_id}.{dataset}.Products_PriceBenchmarks_{merchant_id}`
    ),
    HistoricalPrice AS (
      SELECT
        _PARTITIONDATE as data_date,
        CONCAT(CAST(merchant_id AS STRING), '|', product_id) AS unique_product_id,
        price.value AS price,
        price.currency as price_currency,
        sale_price.value AS sale_price,
        sale_price.currency AS sale_price_currency,
      FROM `{project_id}.{dataset}.Products_{merchant_id}` products
    ),
    HistoricalData AS (
      SELECT
        IFNULL(HistoricalProductMetrics.data_date, HistoricalPrice.data_date) as data_date,
        IFNULL(HistoricalProductMetrics.unique_product_id, HistoricalPrice.unique_product_id) as unique_product_id,
        HistoricalProductMetrics,
        HistoricalPrice,
        HistoricalPriceBenchmarks,
        HistoricalProductStatus,
        CASE
          WHEN HistoricalPriceBenchmarks.price_benchmark_value IS NULL THEN ''
          WHEN (SAFE_DIVIDE(HistoricalPrice.price, HistoricalPriceBenchmarks.price_benchmark_value) - 1) < -0.01 THEN 'Less than PB' -- ASSUMPTION: Enter % as a decimal here
          WHEN (SAFE_DIVIDE(HistoricalPrice.price, HistoricalPriceBenchmarks.price_benchmark_value) - 1) > 0.01 THEN 'More than PB' -- ASSUMPTION: Enter % as a decimal here
          ELSE 'Equal to PB'
        END AS price_competitiveness_band,
      FROM
        HistoricalProductMetrics
      FULL JOIN HistoricalPrice USING (data_date, unique_product_id)
      FULL JOIN HistoricalPriceBenchmarks USING (data_date, unique_product_id)
      FULL JOIN HistoricalProductStatus USING (data_date, unique_product_id)
      WHERE HistoricalPrice.data_date IS NOT NULL
    )
  SELECT
    ProductView,
    HistoricalData.*,
  FROM `{project_id}.{dataset}.product_view_{merchant_id}` ProductView
  LEFT JOIN HistoricalData USING (unique_product_id)
);
