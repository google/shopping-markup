CREATE OR REPLACE VIEW `{project_id}.{dataset}.product_aggregated_category` AS
  SELECT
    pld.account_id,
    account_display_name,
    product_type_l1,
    product_type_l2,
    product_type_l3,
    product_type_l4,
    product_type_l5,
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
    SUM(cost_30_days) AS total_cost_30_days,
  FROM `{project_id}.{dataset}.product_detailed` pld
  GROUP BY
    pld.account_id,
    account_display_name,
    product_type_l1,
    product_type_l2,
    product_type_l3,
    product_type_l4,
    product_type_l5;
