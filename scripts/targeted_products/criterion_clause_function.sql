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

-- Returns where clause for identifying targeted products.

CREATE OR REPLACE FUNCTION `{project_id}.{dataset}.getWhereClause`(criterion STRING)
RETURNS STRING
LANGUAGE js AS """
  const targetConditions = []
  subCriterions = criterion.split('&+');
  for (subCriterion of subCriterions) {{
    subCriterionClause = '1=1';
    if(subCriterion.startsWith('custom')) {{
      const customLabelRegex = /custom(\\d+)/;
      result = subCriterion.split('==');
      
      index = result[0].match(customLabelRegex)[1];
      value = result[1]
      if(value != '*') {{
        value = value.replace('"', '\\"');
        subCriterionClause = 'custom_labels.label_' + index + ' = "' + value + '"';
      }}
    }}
    if(subCriterion.startsWith('brand==')) {{
      result = subCriterion.split('==');
      value = result[1];
      if(value != '*') {{
        subCriterionClause = 'brand = "' + value + '"';
      }}
    }}
    if(subCriterion.startsWith('product_type_')) {{
      const productTypeRegex = /product_type_l(\\d+)/;
      result = subCriterion.split('==');
      
      index = result[0].match(productTypeRegex)[1];
      value = result[1]
      if(value != '*') {{
        value = value.replace('"', '\\"');
        subCriterionClause = 'product_type_l' + index + ' = "' + value + '"';
      }}
    }}
    if(subCriterion.startsWith('category_')) {{
      const categoryRegex = /google_product_category_l(\\d+)/;
      result = subCriterion.split('==');
      
      index = result[0].match(categoryRegex)[1];
      value = result[1]
      if(value != '*') {{
        value = value.replace('"', '\\"');
        subCriterionClause = 'google_product_category_l' + index + ' = "' + value + '"';
      }}
    }}
    if(subCriterion.startsWith('id==')) {{
      result = subCriterion.split('==');
      value = result[1];
      if(value != '*') {{
        subCriterionClause = 'LOWER(offer_id) = "' + value + '"';
      }}
    }}
    if(subCriterion.startsWith('channel==')) {{
      result = subCriterion.split('==');
      value = result[1];
      if(value != '*') {{
        channel = value.split(':')[1];
        subCriterionClause = 'channel = "' + channel + '"';
      }}
    }}
    if(subCriterion.startsWith('channel_exclusivity==')) {{
      result = subCriterion.split('==');
      value = result[1];
      if(value != '*') {{
        channel_exclusivity = value.split(':')[1];
        subCriterionClause = 'channel_exclusivity = "' + channel_exclusivity + '"';
      }}
    }}
    if(subCriterion.startsWith('c_condition==')) {{
      result = subCriterion.split('==');
      value = result[1];
      if(value != '*') {{
        condition = value.split(':')[1];
        subCriterionClause = 'condition = "' + condition + '"';
      }}
    }}
    targetConditions.push(subCriterionClause);
  }}
  return targetConditions.join(' AND ');
  """;
