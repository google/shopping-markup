CREATE OR REPLACE VIEW `{project_id}.{dataset}.product_aggregated_label` AS
  SELECT
    account_id,
    account_display_name,
    custom_label_0,
    custom_label_1,
    custom_label_2,
    custom_label_3,
    custom_label_4,
    COUNT(DISTINCT(unique_product_id)) AS total_products,
    SUM(is_approved) AS total_approved,
    SUM(IF(is_approved = 1 AND in_stock = 1, 1, 0)) AS total_in_stock,
    SUM(IF(is_approved = 1 AND in_stock = 1 AND is_targeted = 1, 1, 0)) AS total_targeted,
    SUM(
      IF(is_approved = 1 AND in_stock = 1 AND is_targeted = 1 AND impressions_30_days > 0, 1, 0)
    ) AS total_products_with_impressions_in_last_30_days,
    SUM(
      IF(is_approved = 1 AND in_stock = 1 AND is_targeted = 1 AND clicks_30_days > 0, 1, 0)
    ) AS total_products_with_clicks_in_last_30_days,
    SUM(impressions_30_days) AS total_impressions_30_days,
    SUM(clicks_30_days) AS total_clicks_30_days,
    SUM(cost_30_days) AS total_cost_30_days
  FROM `{project_id}.{dataset}.product_detailed`
  GROUP BY
    account_id,
    account_display_name,
    custom_label_0,
    custom_label_1,
    custom_label_2,
    custom_label_3,
    custom_label_4;
