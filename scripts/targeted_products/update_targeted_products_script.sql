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

-- Updates targeted products.

DECLARE Criterions ARRAY<STRING>;
DECLARE criterion STRING;
DECLARE where_clause STRING;
DECLARE i INT64 DEFAULT 0;

SET Criterions = (
  WITH DistinctCriterion AS (
    SELECT DISTINCT
      Criteria
    FROM
      `{project_id}.{dataset}.Criteria_{external_customer_id}` AS CriteriaTable
    WHERE
      CriteriaType = 'PRODUCT_PARTITION'
      AND CriteriaTable._DATA_DATE = CriteriaTable._LATEST_DATE
  )
  SELECT
    ARRAY_AGG(Criteria)
  FROM
    DistinctCriterion
);

CREATE OR REPLACE TABLE `{project_id}.{dataset}.StagingTargetedProduct`
 (
   product_id STRING,
   merchant_id INT64
 );

LOOP
  IF i >= ARRAY_LENGTH(Criterions) THEN
    BREAK;
  END IF;
  SET criterion = Criterions[SAFE_OFFSET(i)];
  SET where_clause = `{project_id}.{dataset}.getWhereClause`(criterion);
  EXECUTE IMMEDIATE `{project_id}.{dataset}.getTargetedProductsSql`(where_clause, criterion);
  SET i = i + 1;
END LOOP;

CREATE OR REPLACE TABLE `{project_id}.{dataset}.TargetedProduct`
AS (
  SELECT DISTINCT
    product_id,
    merchant_id
  FROM
    `{project_id}.{dataset}.StagingTargetedProduct`
);
