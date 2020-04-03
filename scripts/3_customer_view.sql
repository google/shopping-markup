CREATE OR REPLACE VIEW `{project_id}.{dataset}.customer_view`
  AS (
    SELECT
      DISTINCT ExternalCustomerId,
      AccountDescriptiveName
    FROM
      `{project_id}.{dataset}.Customer_{external_customer_id}`
    WHERE
      _DATA_DATE IN (
        (
          SELECT
            MAX(_DATA_DATE)
          FROM
            `{project_id}.{dataset}.Customer_{external_customer_id}`
        ))
);
