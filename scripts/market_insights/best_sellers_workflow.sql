# Copyright 2021 Google LLC..
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

CREATE OR REPLACE TABLE `{project_id}.{dataset}.market_insights_best_sellers_materialized` AS (
  WITH
    best_sellers AS (
      SELECT
        _PARTITIONDATE as data_date,
        rank_id,
        rank,
        previous_rank,
        ranking_country,
        ranking_category,
        ranking_category_path.name as ranking_category_path,
        IF(
          ARRAY_LENGTH(SPLIT(ranking_category_path.name, ' > ')) = 1,
          ranking_category_path.name,
          NULL
        ) as ranking_category_name_l1,
        IF(
          ARRAY_LENGTH(SPLIT(ranking_category_path.name, ' > ')) = 2,
          ranking_category_path.name,
          NULL
        ) as ranking_category_name_l2,
        IF(
          ARRAY_LENGTH(SPLIT(ranking_category_path.name, ' > ')) = 3,
          ranking_category_path.name,
          NULL
        ) as ranking_category_name_l3,
        (SELECT ANY_VALUE(name) FROM b.product_title) AS product_title,
        gtins,
        brand,
        google_product_category_path.name as google_product_category_path,
        google_product_category,
        price_range.min,
        price_range.max,
        price_range.currency,
      FROM
        `{project_id}.{dataset}.BestSellers_TopProducts_{merchant_id}` b
      JOIN b.google_product_category_path google_product_category_path
      JOIN b.ranking_category_path ranking_category_path
      JOIN b.product_title product_title
      WHERE
        _PARTITIONDATE = DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
        # Adjust as necessary for other locales
        AND (product_title.locale IN ("en-US") OR product_title.locale IS NULL)
        AND google_product_category_path.locale = "en-US"
        AND ranking_category_path.locale  = "en-US"
    ),
    inventory AS (
      SELECT DISTINCT
        rank_id
      FROM
        `{project_id}.{dataset}.BestSellers_TopProducts_Inventory_{merchant_id}`
      WHERE
        _PARTITIONDATE = DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
    )
  SELECT
    best_sellers.*,
    IF(inventory.rank_id IS NULL, False, True) AS is_in_inventory,
  FROM
    best_sellers
  LEFT JOIN
    inventory
  USING (rank_id)
);
