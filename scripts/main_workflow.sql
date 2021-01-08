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

-- Main workflow to update MarkUp data.
--
-- The script parses target crierion to get the list of targeted products. It
-- then materializes product detailed and product historical tables.

DECLARE criterions ARRAY<STRING>;
DECLARE to_be_processed ARRAY<STRING> DEFAULT [];
DECLARE where_clause STRING;
DECLARE i INT64 DEFAULT 0;
DECLARE BATCH_SIZE INT64 DEFAULT 500;

-- Clean-up existing tables.
DELETE FROM
  `{project_id}.{dataset}.ParsedCriteria_{external_customer_id}`
WHERE 1=1;

DELETE FROM
  `{project_id}.{dataset}.TargetedProduct_{external_customer_id}`
WHERE
  -- Delete data older than 90 days.
  data_date < DATE_SUB(@run_date, INTERVAL 90 DAY)
  OR data_date = @run_date;

SET criterions = (
  WITH DistinctCriterion AS (
    SELECT DISTINCT
      Criteria
    FROM
      `{project_id}.{dataset}.Criteria_{external_customer_id}` AS CriteriaTable
    WHERE
      CriteriaType = 'PRODUCT_PARTITION'
      AND CriteriaTable._DATA_DATE = @run_date
  )
  SELECT
    ARRAY_AGG(Criteria)
  FROM
    DistinctCriterion
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
  EXECUTE IMMEDIATE `{project_id}.{dataset}.constructParsedCriteria_{external_customer_id}`(to_be_processed);
END LOOP;

INSERT `{project_id}.{dataset}.TargetedProduct_{external_customer_id}`
(
  data_date,
  product_id,
  merchant_id
)
WITH TargetedMerchantInfo AS (
  SELECT DISTINCT
    _DATA_DATE AS data_date,
    MerchantId AS merchant_id,
    AdGroupId AS ad_group_id,
    UPPER(GeoTargets.Country_Code) AS target_country
  FROM
    `{project_id}.{dataset}.ShoppingProductStats_{external_customer_id}` AS ShoppingProductStats
  INNER JOIN `{project_id}.{dataset}.geo_targets` GeoTargets
    ON GeoTargets.parent_id = ShoppingProductStats.CountryCriteriaId
  WHERE
    ShoppingProductStats._DATA_DATE = @run_date
), CriteriaInfo AS (
  SELECT
    CriteriaTable._DATA_DATE AS data_date,
    TargetedMerchantInfo.merchant_id,
    TargetedMerchantInfo.target_country,
    CriteriaTable.criteria
  FROM
    TargetedMerchantInfo
  INNER JOIN
    `{project_id}.{dataset}.Criteria_{external_customer_id}` AS CriteriaTable
    ON
      CriteriaTable.AdGroupId = TargetedMerchantInfo.ad_group_id
      AND CriteriaTable._DATA_DATE = TargetedMerchantInfo.data_date
)
SELECT DISTINCT
  ProductView.data_date,
  ProductView.product_id,
  ProductView.merchant_id
FROM
  `{project_id}.{dataset}.product_view_{merchant_id}` AS ProductView
INNER JOIN CriteriaInfo
  ON
    CriteriaInfo.merchant_id = ProductView.merchant_id
    AND CriteriaInfo.target_country = ProductView.target_country
INNER JOIN `{project_id}.{dataset}.ParsedCriteria_{external_customer_id}` AS ParsedCriteria
  ON
    ParsedCriteria.criteria = CriteriaInfo.criteria
    AND (
      ParsedCriteria.custom_label0 IS NULL
      OR TRIM(LOWER(ParsedCriteria.custom_label0)) = TRIM(LOWER(ProductView.custom_labels.label_0)))
    AND (
      ParsedCriteria.custom_label1 IS NULL
      OR TRIM(LOWER(ParsedCriteria.custom_label1)) = TRIM(LOWER(ProductView.custom_labels.label_1)))
    AND (
      ParsedCriteria.custom_label2 IS NULL
      OR TRIM(LOWER(ParsedCriteria.custom_label2)) = TRIM(LOWER(ProductView.custom_labels.label_2)))
    AND (
      ParsedCriteria.custom_label3 IS NULL
      OR TRIM(LOWER(ParsedCriteria.custom_label3)) = TRIM(LOWER(ProductView.custom_labels.label_3)))
    AND (
      ParsedCriteria.custom_label4 IS NULL
      OR TRIM(LOWER(ParsedCriteria.custom_label4)) = TRIM(LOWER(ProductView.custom_labels.label_4)))
    AND (
      ParsedCriteria.product_type_l1 IS NULL
      OR TRIM(LOWER(ParsedCriteria.product_type_l1)) = TRIM(LOWER(ProductView.product_type_l1)))
    AND (
      ParsedCriteria.product_type_l2 IS NULL
      OR TRIM(LOWER(ParsedCriteria.product_type_l2)) = TRIM(LOWER(ProductView.product_type_l2)))
    AND (
      ParsedCriteria.product_type_l3 IS NULL
      OR TRIM(LOWER(ParsedCriteria.product_type_l3)) = TRIM(LOWER(ProductView.product_type_l3)))
    AND (
      ParsedCriteria.product_type_l4 IS NULL
      OR TRIM(LOWER(ParsedCriteria.product_type_l4)) = TRIM(LOWER(ProductView.product_type_l4)))
    AND (
      ParsedCriteria.product_type_l5 IS NULL
      OR TRIM(LOWER(ParsedCriteria.product_type_l5)) = TRIM(LOWER(ProductView.product_type_l5)))
    AND (
      ParsedCriteria.google_product_category_l1 IS NULL
      OR TRIM(LOWER(ParsedCriteria.google_product_category_l1)) = TRIM(LOWER(ProductView.google_product_category_l1)))
    AND (
      ParsedCriteria.google_product_category_l2 IS NULL
      OR TRIM(LOWER(ParsedCriteria.google_product_category_l2)) = TRIM(LOWER(ProductView.google_product_category_l2)))
    AND (
      ParsedCriteria.google_product_category_l3 IS NULL
      OR TRIM(LOWER(ParsedCriteria.google_product_category_l3)) = TRIM(LOWER(ProductView.google_product_category_l3)))
    AND (
      ParsedCriteria.google_product_category_l4 IS NULL
      OR TRIM(LOWER(ParsedCriteria.google_product_category_l4)) = TRIM(LOWER(ProductView.google_product_category_l4)))
    AND (
      ParsedCriteria.google_product_category_l5 IS NULL
      OR TRIM(LOWER(ParsedCriteria.google_product_category_l5)) = TRIM(LOWER(ProductView.google_product_category_l5)))
    AND (
      ParsedCriteria.brand IS NULL
      OR TRIM(LOWER(ParsedCriteria.brand)) = TRIM(LOWER(ProductView.brand)))
    AND (
      ParsedCriteria.offer_id IS NULL
      OR TRIM(LOWER(ParsedCriteria.offer_id)) = TRIM(LOWER(ProductView.offer_id)))
    AND (
      ParsedCriteria.channel IS NULL
      OR TRIM(LOWER(ParsedCriteria.channel)) = TRIM(LOWER(ProductView.channel)))
    AND (
      ParsedCriteria.channel_exclusivity IS NULL
      OR TRIM(LOWER(ParsedCriteria.channel_exclusivity)) = TRIM(LOWER(ProductView.channel_exclusivity)))
    AND (
      ParsedCriteria.condition IS NULL
      OR TRIM(LOWER(ParsedCriteria.condition)) = TRIM(LOWER(ProductView.condition)))
WHERE
  ProductView.data_date = @run_date;


-- Update product detailed and product historical materialized tables.
CALL `{project_id}.{dataset}.product_detailed_proc`();
CALL `{project_id}.{dataset}.product_historical_proc`();
