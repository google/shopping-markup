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

-- Backfill main workflow.
--
-- The script parses target crierion to get the list of targeted products. It
-- then materializes product detailed and product historical tables. The script
-- uses @run_date parameter and hence can be backfilled on a specific date. This
-- is useful when a Google Ads or GMC data transfer has failed on a specific
-- day.

DECLARE to_be_processed ARRAY<STRING> DEFAULT [];
DECLARE where_clause STRING;
DECLARE i INT64 DEFAULT 0;
DECLARE BATCH_SIZE INT64 DEFAULT 500;
DECLARE total_criterions DEFAULT 0;

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

CREATE TEMPORARY TABLE IF NOT EXISTS DistinctCriterion AS (
  WITH DistinctCriterion AS (
    SELECT DISTINCT
      Criteria
    FROM
      `{project_id}.{dataset}.Criteria_{external_customer_id}` AS CriteriaTable
    WHERE
      CriteriaType = 'PRODUCT_PARTITION'
      -- If the run_date is not a backfill then use the latest available data.
      AND (
        (
          @run_date = CURRENT_DATE()
          AND CriteriaTable._DATA_DATE = CriteriaTable._LATEST_DATE)
        OR (
          @run_date <> CURRENT_DATE()
          AND CriteriaTable._DATA_DATE = @run_date))
  )
  SELECT
    Criteria,
    ROW_NUMBER() OVER (ORDER BY Criteria asc) as RowNum
  FROM
    DistinctCriterion
);

SET total_criterions = (SELECT COUNT(1) FROM DistinctCriterion);

LOOP
  IF i >= total_criterions THEN
    BREAK;
  END IF;
  SET to_be_processed = (
    SELECT
      ARRAY_AGG(Criteria)
    FROM
      DistinctCriterion
    WHERE
      RowNum BETWEEN i AND i+BATCH_SIZE
  );
  SET i = i + BATCH_SIZE + 1;
  EXECUTE IMMEDIATE `{project_id}.{dataset}.constructParsedCriteria_{external_customer_id}`(to_be_processed);
END LOOP;


CREATE TEMP TABLE CriteriaInfo
AS (
  WITH TargetedMerchantInfo AS (
    SELECT DISTINCT
      MerchantId AS merchant_id,
      AdGroupId AS ad_group_id,
      UPPER(GeoTargets.Country_Code) AS target_country
    FROM
      `{project_id}.{dataset}.ShoppingProductStats_{external_customer_id}` AS ShoppingProductStats
    INNER JOIN `{project_id}.{dataset}.geo_targets` GeoTargets
      ON GeoTargets.parent_id = ShoppingProductStats.CountryCriteriaId
    WHERE
      -- If the run_date is not a backfill then use the latest available data.
      (
        (
          @run_date = CURRENT_DATE()
          AND ShoppingProductStats._DATA_DATE = ShoppingProductStats._LATEST_DATE)
        OR (
          @run_date <> CURRENT_DATE()
          AND ShoppingProductStats._DATA_DATE = @run_date))
  )
  SELECT
    TargetedMerchantInfo.merchant_id,
    TargetedMerchantInfo.target_country,
    CriteriaTable.criteria
  FROM
    TargetedMerchantInfo
  INNER JOIN
    `{project_id}.{dataset}.Criteria_{external_customer_id}` AS CriteriaTable
    ON
      CriteriaTable.AdGroupId = TargetedMerchantInfo.ad_group_id
      -- If the run_date is not a backfill then use the latest available data.
      AND (
        (
          @run_date = CURRENT_DATE()
          AND CriteriaTable._DATA_DATE = CriteriaTable._LATEST_DATE)
        OR (
          @run_date <> CURRENT_DATE()
          AND CriteriaTable._DATA_DATE = @run_date))
);

CREATE TEMP TABLE IdTargetedOffer
AS (
  SELECT DISTINCT
    CriteriaInfo.merchant_id,
    CriteriaInfo.target_country,
    ParsedCriteria.offer_id
  FROM
    `{project_id}.{dataset}.ParsedCriteria_{external_customer_id}` ParsedCriteria
  INNER JOIN CriteriaInfo
    ON ParsedCriteria.criteria = CriteriaInfo.criteria
  WHERE
    ParsedCriteria.offer_id IS NOT NULL
);


CREATE TEMP TABLE IdTargeted
AS (
  SELECT
    ProductView.data_date,
    ProductView.product_id,
    ProductView.merchant_id,
    ProductView.target_country
  FROM
    `{project_id}.{dataset}.product_view_{merchant_id}` AS ProductView
  INNER JOIN IdTargetedOffer
    ON
      IdTargetedOffer.merchant_id = ProductView.merchant_id
      AND IdTargetedOffer.target_country = ProductView.target_country
      AND IdTargetedOffer.offer_id = ProductView.offer_id
  WHERE
    -- If the run_date is not a backfill then use the latest available data.
      (
        (
          @run_date = CURRENT_DATE()
          AND ProductView.data_date = ProductView.latest_date)
        OR (
          @run_date <> CURRENT_DATE()
          AND ProductView.data_date = @run_date))
);

CREATE TEMP TABLE NonIdTargeted
AS (
  SELECT
    ProductView.data_date,
    ProductView.product_id,
    ProductView.merchant_id,
    ProductView.target_country
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
        ParsedCriteria.channel IS NULL
        OR TRIM(LOWER(ParsedCriteria.channel)) = TRIM(LOWER(ProductView.channel)))
      AND (
        ParsedCriteria.channel_exclusivity IS NULL
        OR TRIM(LOWER(ParsedCriteria.channel_exclusivity)) = TRIM(LOWER(ProductView.channel_exclusivity)))
      AND (
        ParsedCriteria.condition IS NULL
        OR TRIM(LOWER(ParsedCriteria.condition)) = TRIM(LOWER(ProductView.condition)))
  WHERE
    ParsedCriteria.offer_id IS NULL
    -- If the run_date is not a backfill then use the latest available data.
    AND (
      (
        @run_date = CURRENT_DATE()
        AND ProductView.data_date = ProductView.latest_date)
      OR (
        @run_date <> CURRENT_DATE()
        AND ProductView.data_date = @run_date))
);

INSERT `{project_id}.{dataset}.TargetedProduct_{external_customer_id}`
(
  data_date,
  product_id,
  merchant_id,
  target_country
)
SELECT
  data_date,
  product_id,
  merchant_id,
  target_country
FROM
  IdTargeted
UNION ALL
SELECT
  data_date,
  product_id,
  merchant_id,
  target_country
FROM
  NonIdTargeted;


-- Update product detailed and product historical materialized tables.
CALL `{project_id}.{dataset}.product_detailed_proc`();
CALL `{project_id}.{dataset}.product_historical_proc`();
