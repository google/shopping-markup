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

-- Stored procedure for materializing product detailed view.
--
-- Materialization will help in reducing the query cost from the complex joins
-- present in the underlying product_detailed_view.

CREATE OR REPLACE PROCEDURE `{project_id}.{dataset}.product_detailed_proc`()
BEGIN
  CREATE OR REPLACE TABLE `{project_id}.{dataset}.product_detailed_materialized`
  AS (
    SELECT
      *
    FROM
      `{project_id}.{dataset}.product_detailed_view`
    WHERE
      data_date IN (
        (
          SELECT
            MAX(data_date)
          FROM
            `{project_id}.{dataset}.product_detailed_view`
        ))
  );
END;

CALL `{project_id}.{dataset}.product_detailed_proc`();
