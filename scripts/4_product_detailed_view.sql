CREATE OR REPLACE VIEW
  `{project_id}.{dataset}.product_detailed` AS
WITH
  ProductIssuesTable AS (
  SELECT
    merchant_id,
    unique_product_id,
    servability
  FROM
    `{project_id}.{dataset}.product_view`,
    UNNEST(issues) issues )
SELECT
  COALESCE(product_view.aggregator_id, product_view.merchant_id) AS account_id,
  MAX(customer_view.accountdescriptivename) AS account_display_name,
  product_view.merchant_id AS sub_account_id,
  product_view.unique_product_id,
  MAX(product_view.offer_id) AS offer_id,
  MAX(product_view.in_stock) AS in_stock,
  MIN(CASE
      WHEN LOWER(destinations.status) <> 'approved' THEN 0
    ELSE
    1
  END
    ) AS is_approved,
  MIN(CASE
      WHEN servability IS NOT NULL AND LOWER(servability) <> 'unaffected' THEN 0
    ELSE
    1
  END
    ) AS is_targeted,
  MAX(title) AS title,
  MAX(link) AS item_url,
  MAX(product_type_l1) AS product_type_l1,
  MAX(product_type_l2) AS product_type_l2,
  MAX(product_type_l3) AS product_type_l3,
  MAX(product_type_l4) AS product_type_l4,
  MAX(product_type_l5) AS product_type_l5,
  MAX(custom_labels.label_0) AS custom_label_0,
  MAX(custom_labels.label_1) AS custom_label_1,
  MAX(custom_labels.label_2) AS custom_label_2,
  MAX(custom_labels.label_3) AS custom_label_3,
  MAX(custom_labels.label_4) AS custom_label_4,
  MAX(product_view.brand) AS brand,
  SUM(product_metrics_view.impressions_30_days) AS impressions_30_days,
  SUM(product_metrics_view.clicks_30_days) AS clicks_30_days,
  SUM(product_metrics_view.cost_30_days) AS cost_30_days,
  ANY_VALUE(issues) AS issues
FROM
  `{project_id}.{dataset}.product_view` product_view,
  UNNEST(destinations) AS destinations
LEFT JOIN
  ProductIssuesTable
ON
  ProductIssuesTable.merchant_id = product_view.merchant_id
  AND ProductIssuesTable.unique_product_id = product_view.unique_product_id
LEFT JOIN
  `{project_id}.{dataset}.product_metrics_view` product_metrics_view
ON
  product_metrics_view.merchantid = product_view.merchant_id
  AND LOWER(product_metrics_view.product_id) = LOWER(product_view.product_id)
LEFT JOIN
  `{project_id}.{dataset}.customer_view` customer_view
ON
  customer_view.externalcustomerid = product_metrics_view.externalcustomerid
GROUP BY
  account_id,
  product_view.merchant_id,
  product_view.unique_product_id;
