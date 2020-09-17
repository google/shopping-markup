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

-- Returns SQL to identify targeted products.

CREATE OR REPLACE FUNCTION `{project_id}.{dataset}.getTargetedProductsSql`(
  whereClause STRING)
RETURNS STRING
LANGUAGE js AS """
    const sql = `
      CREATE OR REPLACE TABLE \\`{project_id}.{dataset}.StagingTargetedProduct\\`
      AS (
        WITH TargetedMerchantInfo AS (
          SELECT DISTINCT
            MerchantId AS merchant_id,
            AdGroupId AS ad_group_id,
            UPPER(GeoTargets.Country_Code) AS target_country
          FROM
            \\`{project_id}.{dataset}.ShoppingProductStats_{external_customer_id}\\` AS ShoppingProductStats
          INNER JOIN \\`{project_id}.{dataset}.geo_targets\\` GeoTargets
            ON GeoTargets.parent_id = ShoppingProductStats.CountryCriteriaId
          WHERE
            ShoppingProductStats._DATA_DATE = ShoppingProductStats._LATEST_DATE
        ), CriteriaInfo AS (
          SELECT
            TargetedMerchantInfo.merchant_id,
            TargetedMerchantInfo.target_country,
            CriteriaTable.criteria
          FROM
            TargetedMerchantInfo
          INNER JOIN
            \\`{project_id}.{dataset}.Criteria_{external_customer_id}\\` AS CriteriaTable
            ON
              CriteriaTable.AdGroupId = TargetedMerchantInfo.ad_group_id
              AND CriteriaTable._DATA_DATE = CriteriaTable._LATEST_DATE
        )
        SELECT DISTINCT
          product_id,
          merchant_id
        FROM
          \\`{project_id}.{dataset}.StagingTargetedProduct\\`
        UNION DISTINCT
        SELECT DISTINCT
          ProductView.product_id,
          ProductView.merchant_id
        FROM
          \\`{project_id}.{dataset}.product_view\\` AS ProductView
        INNER JOIN CriteriaInfo
          ON
            CriteriaInfo.merchant_id = ProductView.merchant_id
            AND CriteriaInfo.target_country = ProductView.target_country
        WHERE
          {{whereClause}}
      );
    `;
    return sql.replace(/{{whereClause}}/g, whereClause);
  """;
