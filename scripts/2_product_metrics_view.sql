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

CREATE OR REPLACE VIEW `{project_id}.{dataset}.product_metrics_view`
  AS (
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
      )
    SELECT
      externalcustomerid,
      merchantid,
      CONCAT(
        channel,
        ':',
        LanguageTable.language_code,
        ':',
        CountryTable.country_code,
        ':',
        offerid) AS product_id,
      SUM(impressions) AS impressions_30_days,
      SUM(clicks) AS clicks_30_days,
      SUM(cost) AS cost_30_days,
      SUM(conversions) AS conversions_30_days,
      SUM(ConversionValue) AS conversions_value_30_days,
    FROM
      `{project_id}.{dataset}.ShoppingProductStats_{external_customer_id}` AS ShoppingProductStats
    INNER JOIN
      CountryTable
      ON CountryTable.country_criterion = ShoppingProductStats.countrycriteriaid
    INNER JOIN
      LanguageTable
      ON CAST(LanguageTable.language_criterion AS STRING) = ShoppingProductStats.languagecriteriaid
    WHERE
      _DATA_DATE >= DATE_SUB(_LATEST_DATE, INTERVAL 30 DAY)
    GROUP BY
      externalcustomerid,
      merchantid,
      product_id
);
