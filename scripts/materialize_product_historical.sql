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

-- Stored procedure for creating historic snapshot at a product category level.

CREATE OR REPLACE PROCEDURE `{project_id}.{dataset}.product_historical_proc`()
BEGIN
  CREATE OR REPLACE TABLE `{project_id}.{dataset}.product_historical_materialized`
  AS (
    SELECT
      data_date,
      account_id,
      product_type_l1,
      product_type_l2,
      product_type_l3,
      product_type_l4,
      product_type_l5,
      COUNT(1) AS total_products,
      SUM(is_approved) AS total_approved,
      SUM(funnel_in_stock) AS total_in_stock,
      SUM(funnel_targeted) AS total_targeted,
      SUM(funnel_has_impression) AS total_products_with_impressions_in_30_days,
      SUM(funnel_has_clicks) AS total_products_with_clicks_in_30_days,
      IFNULL(SUM(impressions_30_days), 0) AS total_impressions_30_days,
      IFNULL(SUM(clicks_30_days), 0) AS total_clicks_30_days,
      IFNULL(SUM(cost_30_days), 0) AS total_cost_30_days
    FROM
      `{project_id}.{dataset}.product_detailed_view`
    WHERE
      data_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
    GROUP BY
      data_date,
      account_id,
      product_type_l1,
      product_type_l2,
      product_type_l3,
      product_type_l4,
      product_type_l5
  );
END;

CALL `{project_id}.{dataset}.product_historical_proc`();
