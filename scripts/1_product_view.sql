-- Creates a latest snapshot view of products.
--
-- The Products_<Merchant Id> table has product data partitioned by date.
-- This view will get latest product data and create derived columns useful
-- for further processing of data.

CREATE OR REPLACE VIEW `{project_id}.{dataset}.product_view`
AS (
  SELECT
    product_id,
    merchant_id,
    aggregator_id,
    offer_id,
    title,
    description,
    link,
    mobile_link,
    image_link,
    additional_image_links,
    content_language,
    target_country,
    channel,
    expiration_date,
    google_expiration_date,
    adult,
    age_group,
    availability,
    availability_date,
    brand,
    color,
    condition,
    custom_labels,
    gender,
    gtin,
    item_group_id,
    material,
    mpn,
    pattern,
    price,
    sale_price,
    google_product_category,
    google_product_category_path,
    product_type,
    additional_product_types,
    destinations,
    issues,
    CONCAT(CAST(merchant_id AS STRING), '|', product_id) AS unique_product_id,
    IFNULL(SPLIT(product_type, '>')[SAFE_OFFSET(0)], 'N/A') AS product_type_l1,
    IFNULL(SPLIT(product_type, '>')[SAFE_OFFSET(1)], 'N/A') AS product_type_l2,
    IFNULL(SPLIT(product_type, '>')[SAFE_OFFSET(2)], 'N/A') AS product_type_l3,
    IFNULL(SPLIT(product_type, '>')[SAFE_OFFSET(3)], 'N/A') AS product_type_l4,
    IFNULL(SPLIT(product_type, '>')[SAFE_OFFSET(4)], 'N/A') AS product_type_l5,
    IF(availability = 'in stock', 1, 0) AS in_stock,
    CASE
      WHEN LOWER(destinations.status) <> 'approved' THEN 0
      ELSE 1
    END AS is_approved,
    CASE
      WHEN servability IS NOT NULL AND LOWER(servability) <> 'unaffected' THEN 0
      ELSE 1
    END AS is_targeted
  FROM
    `{project_id}.{dataset}.Products_{merchant_id}`
  WHERE
    _PARTITIONDATE IN (
      (
        SELECT
          MAX(_PARTITIONDATE)
        FROM
          `{project_id}.{dataset}.Products_{merchant_id}`
      ))
);
