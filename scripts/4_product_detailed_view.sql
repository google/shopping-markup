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

-- Creates a latest snapshot view of products combined with performance metrics.
CREATE OR REPLACE VIEW
  `{project_id}.{dataset}.product_detailed_view` AS
WITH
  ProductMetrics AS (
    SELECT
      product_view.data_date,
      product_view.unique_product_id,
      product_metrics_view.externalcustomerid,
      product_view.target_country,
      SUM(product_metrics_view.impressions) AS impressions_30_days,
      SUM(product_metrics_view.clicks) AS clicks_30_days,
      SUM(product_metrics_view.cost) AS cost_30_days,
      SUM(product_metrics_view.conversions) AS conversions_30_days,
      SUM(product_metrics_view.conversions_value) AS conversions_value_30_days
    FROM
      `{project_id}.{dataset}.product_metrics_view` product_metrics_view
    INNER JOIN
      `{project_id}.{dataset}.product_view_{merchant_id}` product_view
      ON
        product_metrics_view.merchantid = product_view.merchant_id
        AND LOWER(product_metrics_view.channel) = LOWER(product_view.channel)
        AND LOWER(product_metrics_view.language_code) = LOWER(product_view.content_language)
        AND LOWER(product_metrics_view.country_code) = LOWER(product_view.target_country)
        AND LOWER(product_metrics_view.offer_id) = LOWER(product_view.offer_id)
        AND product_metrics_view.data_date
          BETWEEN DATE_SUB(product_view.data_date, INTERVAL 30 DAY)
          AND product_view.data_date
    GROUP BY
      data_date,
      unique_product_id,
      externalcustomerid,
      target_country
  ),
  ProductData AS (
    SELECT
      product_view.data_date,
      product_view.latest_date,
      COALESCE(product_view.aggregator_id, product_view.merchant_id) AS account_id,
      MAX(customer_view.accountdescriptivename) AS account_display_name,
      product_view.merchant_id AS sub_account_id,
      product_view.unique_product_id,
      product_view.target_country,
      MAX(product_view.offer_id) AS offer_id,
      MAX(product_view.channel) AS channel,
      MAX(product_view.in_stock) AS in_stock,
      # An offer is labeled as approved when able to serve on all destinations
      MAX(is_approved) AS is_approved,
      # Aggregated Issues & Servability Statuses
      MAX(disapproval_issues) as disapproval_issues,
      MAX(demotion_issues) as demotion_issues,
      MAX(warning_issues) as warning_issues,
      MIN(IF(TargetedProduct.product_id IS NULL, 0, 1)) AS is_targeted,
      MAX(title) AS title,
      MAX(link) AS item_url,
      MAX(product_type_l1) AS product_type_l1,
      MAX(product_type_l2) AS product_type_l2,
      MAX(product_type_l3) AS product_type_l3,
      MAX(product_type_l4) AS product_type_l4,
      MAX(product_type_l5) AS product_type_l5,
      MAX(google_product_category_l1) AS google_product_category_l1,
      MAX(google_product_category_l2) AS google_product_category_l2,
      MAX(google_product_category_l3) AS google_product_category_l3,
      MAX(google_product_category_l4) AS google_product_category_l4,
      MAX(google_product_category_l5) AS google_product_category_l5,
      MAX(custom_labels.label_0) AS custom_label_0,
      MAX(custom_labels.label_1) AS custom_label_1,
      MAX(custom_labels.label_2) AS custom_label_2,
      MAX(custom_labels.label_3) AS custom_label_3,
      MAX(custom_labels.label_4) AS custom_label_4,
      MAX(product_view.brand) AS brand,
      MAX(ProductMetrics.impressions_30_days) AS impressions_30_days,
      MAX(ProductMetrics.clicks_30_days) AS clicks_30_days,
      MAX(ProductMetrics.cost_30_days) AS cost_30_days,
      MAX(ProductMetrics.conversions_30_days) AS conversions_30_days,
      MAX(ProductMetrics.conversions_value_30_days) AS conversions_value_30_days,
      MAX(description) AS description,
      MAX(mobile_link) AS mobile_link,
      MAX(image_link) AS image_link,
      ANY_VALUE(additional_image_links) AS additional_image_links,
      MAX(content_language) AS content_language,
      MAX(expiration_date) AS expiration_date,
      MAX(google_expiration_date) AS google_expiration_date,
      MAX(adult) AS adult,
      MAX(age_group) AS age_group,
      MAX(availability) AS availability,
      MAX(availability_date) AS availability_date,
      MAX(color) AS color,
      MAX(condition) AS condition,
      MAX(gender) AS gender,
      MAX(gtin) AS gtin,
      MAX(item_group_id) AS item_group_id,
      MAX(material) AS material,
      MAX(mpn) AS mpn,
      MAX(pattern) AS pattern,
      ANY_VALUE(price) AS price,
      ANY_VALUE(sale_price) AS sale_price,
      MAX(sale_price_effective_start_date) AS sale_price_effective_start_date,
      MAX(sale_price_effective_end_date) AS sale_price_effective_end_date,
      ANY_VALUE(additional_product_types) AS additional_product_types
    FROM
      `{project_id}.{dataset}.product_view_{merchant_id}` product_view
    LEFT JOIN
      ProductMetrics
      ON
        ProductMetrics.data_date = product_view.data_date
        AND ProductMetrics.unique_product_id = product_view.unique_product_id
        AND ProductMetrics.target_country = product_view.target_country
    LEFT JOIN
      `{project_id}.{dataset}.customer_view` customer_view
      ON
        customer_view.externalcustomerid = ProductMetrics.externalcustomerid
        AND customer_view.data_date = ProductMetrics.data_date
    LEFT JOIN
      `{project_id}.{dataset}.TargetedProduct_{external_customer_id}` TargetedProduct
      ON
        TargetedProduct.merchant_id = product_view.merchant_id
        AND TargetedProduct.product_id = product_view.product_id
        AND TargetedProduct.data_date = product_view.data_date
        AND TargetedProduct.target_country = product_view.target_country
    GROUP BY
      data_date,
      latest_date,
      account_id,
      product_view.merchant_id,
      product_view.unique_product_id,
      target_country
  )
SELECT
  *,
  CASE
    WHEN is_approved = 1 AND in_stock = 1
      THEN 1
    ELSE 0
  END AS funnel_in_stock,
  CASE
    WHEN is_approved = 1 AND in_stock = 1  AND is_targeted = 1
      THEN 1
    ELSE 0
  END AS funnel_targeted,
  CASE
    WHEN
      is_approved = 1
      AND in_stock = 1
      AND is_targeted = 1
      AND impressions_30_days > 0
      THEN 1
    ELSE 0
  END AS funnel_has_impression,
  CASE
    WHEN
      is_approved = 1
      AND in_stock = 1
      AND is_targeted = 1
      AND impressions_30_days > 0
      AND clicks_30_days > 0
      THEN 1
    ELSE 0
  END AS funnel_has_clicks
FROM
  ProductData;
