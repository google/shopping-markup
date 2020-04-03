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
      SUM(cost) AS cost_30_days
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
