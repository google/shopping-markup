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

DECLARE criterions ARRAY<STRING>;
DECLARE to_be_processed ARRAY<STRING> DEFAULT [];
DECLARE where_clause STRING;
DECLARE i INT64 DEFAULT 0;
DECLARE BATCH_SIZE INT64 DEFAULT 50;

SET criterions = (
  WITH DistinctCriterion AS (
    SELECT DISTINCT
      Criteria
    FROM
      `{project_id}.{dataset}.Criteria_6583794055` AS CriteriaTable
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
  IF i >= ARRAY_LENGTH(criterions) THEN
    BREAK;
  END IF;
  SET to_be_processed = (
    SELECT ARRAY_AGG(part ORDER BY index)
    FROM UNNEST(criterions) part WITH OFFSET index
    WHERE index BETWEEN i AND i+BATCH_SIZE
  );
  SET i = i + BATCH_SIZE + 1;
  SET where_clause = `{project_id}.{dataset}.getWhereClause`(to_be_processed);
  IF where_clause = '' THEN
    BREAK;
  END IF;
  EXECUTE IMMEDIATE `{project_id}.{dataset}.getTargetedProductsSql`(where_clause);
END LOOP;

CREATE OR REPLACE TABLE `{project_id}.{dataset}.TargetedProduct`
AS (
  SELECT DISTINCT
    product_id,
    merchant_id
  FROM
    `{project_id}.{dataset}.StagingTargetedProduct`
);
