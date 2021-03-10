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

CREATE OR REPLACE VIEW `{project_id}.{dataset}.customer_view`
  AS (
    WITH
      LatestDate AS (
        SELECT
          MAX(_DATA_DATE) AS latest_date
        FROM
          `{project_id}.{dataset}.Customer_{external_customer_id}`
      )
    SELECT DISTINCT
      _DATA_DATE AS data_date,
      LatestDate.latest_date,
      ExternalCustomerId,
      AccountDescriptiveName
    FROM
      `{project_id}.{dataset}.Customer_{external_customer_id}`,
      LatestDate
);
