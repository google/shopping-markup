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
  whereClause STRING,
  criterion STRING)
RETURNS STRING
LANGUAGE js AS """
    const sql = `
      CREATE OR REPLACE TABLE \\`{project_id}.{dataset}.StagingTargetedProduct\\`
      AS (
        SELECT DISTINCT
          product_id,
          merchant_id
        FROM
          \\`{project_id}.{dataset}.StagingTargetedProduct\\`
        UNION DISTINCT
        SELECT DISTINCT
          product_id,
          merchant_id
        FROM
          \\`{project_id}.{dataset}.product_view\\`
        WHERE
          merchant_id IN (
            (
              SELECT DISTINCT
                MerchantId
              FROM
                \\`{project_id}.{dataset}.Criteria_{external_customer_id}\\` AS CriteriaTable
              INNER JOIN \\`{project_id}.{dataset}.ShoppingProductStats_{external_customer_id}\\` AS ShoppingProductStats
                ON  CriteriaTable.AdGroupId = ShoppingProductStats.AdGroupId
              WHERE
                CriteriaTable._DATA_DATE = CriteriaTable._LATEST_DATE
                AND ShoppingProductStats._DATA_DATE = ShoppingProductStats._LATEST_DATE
                AND CriteriaTable.Criteria = "{{criterion}}"
            ))
          AND target_country IN (
            (
              SELECT DISTINCT
                UPPER(GeoTargets.Country_Code)
              FROM
                \\`{project_id}.{dataset}.Criteria_{external_customer_id}\\` AS CriteriaTable
              INNER JOIN \\`{project_id}.{dataset}.ShoppingProductStats_{external_customer_id}\\` AS ShoppingProductStats
                ON  CriteriaTable.AdGroupId = ShoppingProductStats.AdGroupId
              INNER JOIN \\`{project_id}.{dataset}.geo_targets\\` GeoTargets
                ON GeoTargets.parent_id = ShoppingProductStats.CountryCriteriaId
              WHERE
                CriteriaTable._DATA_DATE = CriteriaTable._LATEST_DATE
                AND ShoppingProductStats._DATA_DATE = ShoppingProductStats._LATEST_DATE
                AND CriteriaTable.Criteria = "{{criterion}}"
            ))
          AND {{whereClause}}
      );
    `;
    return sql.replace(/{{criterion}}/g, criterion).replace(/{{whereClause}}/g, whereClause);
  """;