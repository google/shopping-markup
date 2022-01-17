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

-- Constructs insert statements to populate ParsedCriteria table.
--
-- @param criterions Array of product group criterions.
-- @return String containing insert sql statements to construct ParsedCriteria
--    table.
CREATE OR REPLACE FUNCTION `{project_id}.{dataset}.constructParsedCriteria_{external_customer_id}`(criterions ARRAY<STRING>)
RETURNS STRING
LANGUAGE js AS """
  function getParsedCriteria(criterion) {{
    let parsedCriteria = {{}}
    parsedCriteria['criteria']  = criterion;
    subCriterions = criterion.split('&+');
    for (subCriterion of subCriterions) {{
      if(subCriterion.startsWith('custom')) {{
        const customLabelRegex = /custom(\\d+)/;
        result = subCriterion.split('==');

        index = result[0].match(customLabelRegex)[1];
        value = result[1]
        if(value != '*') {{
          parsedCriteria['custom_label' + index] = value;
        }}
      }}
      if(subCriterion.startsWith('brand==')) {{
        result = subCriterion.split('==');
        value = result[1];
        if(value != '*') {{
          parsedCriteria['brand'] = value;
        }}
      }}
      if(subCriterion.startsWith('product_type_')) {{
        const productTypeRegex = /product_type_l(\\d+)/;
        result = subCriterion.split('==');

        index = result[0].match(productTypeRegex)[1];
        value = result[1]
        if(value != '*') {{
          parsedCriteria['product_type_l' + index] = value;
        }}
      }}
      if(subCriterion.startsWith('category_')) {{
        const categoryRegex = /category_l(\\d+)/;
        result = subCriterion.split('==');

        index = result[0].match(categoryRegex)[1];
        value = result[1]
        if(value != '*') {{
          parsedCriteria['google_product_category_l' + index] = value;
        }}
      }}
      if(subCriterion.startsWith('id==')) {{
        result = subCriterion.split('==');
        value = result[1];
        if(value != '*') {{
          parsedCriteria['offer_id'] = value;
        }}
      }}
      if(subCriterion.startsWith('channel==')) {{
        result = subCriterion.split('==');
        value = result[1];
        if(value != '*') {{
          channel = value.split(':')[1];
          parsedCriteria['channel'] = channel;
        }}
      }}
      if(subCriterion.startsWith('channel_exclusivity==')) {{
        result = subCriterion.split('==');
        value = result[1];
        if(value != '*') {{
          channel_exclusivity = value.split(':')[1];
          parsedCriteria['channel_exclusivity'] = channel_exclusivity;
        }}
      }}
      if(subCriterion.startsWith('c_condition==')) {{
        result = subCriterion.split('==');
        value = result[1];
        if(value != '*') {{
          condition = value.split(':')[1];
          parsedCriteria['condition'] = condition;
        }}
      }}
    }}
    return parsedCriteria;
  }}
  sql = 'INSERT INTO `{project_id}.{dataset}.ParsedCriteria_{external_customer_id}` VALUES ';
  i = 0;
  for (criterion of criterions) {{
    criterion = criterion.replace(/"/g, '\\\\"');
    parsedCriteria = getParsedCriteria(criterion)
    if ( i!=0 ) {{
      sql += ',';
    }}
    sql += '('
    sql += '"' + criterion + '",';
    sql += (parsedCriteria['custom_label0'] ? '"' + parsedCriteria['custom_label0'] + '"' : 'NULL') + ',';
    sql += (parsedCriteria['custom_label1'] ? '"' + parsedCriteria['custom_label1'] + '"' : 'NULL') + ',';
    sql += (parsedCriteria['custom_label2'] ? '"' + parsedCriteria['custom_label2'] + '"' : 'NULL') + ',';
    sql += (parsedCriteria['custom_label3'] ? '"' + parsedCriteria['custom_label3'] + '"' : 'NULL') + ',';
    sql += (parsedCriteria['custom_label4'] ? '"' + parsedCriteria['custom_label4'] + '"' : 'NULL') + ',';
    sql += (parsedCriteria['product_type_l1'] ? '"' + parsedCriteria['product_type_l1'] + '"' : 'NULL') + ',';
    sql += (parsedCriteria['product_type_l2'] ? '"' + parsedCriteria['product_type_l2'] + '"' : 'NULL') + ',';
    sql += (parsedCriteria['product_type_l3'] ? '"' + parsedCriteria['product_type_l3'] + '"' : 'NULL') + ',';
    sql += (parsedCriteria['product_type_l4'] ? '"' + parsedCriteria['product_type_l4'] + '"' : 'NULL') + ',';
    sql += (parsedCriteria['product_type_l5'] ? '"' + parsedCriteria['product_type_l5'] + '"' : 'NULL') + ',';
    sql += (parsedCriteria['google_product_category_l1'] ? '"' + parsedCriteria['google_product_category_l1'] + '"' : 'NULL') + ',';
    sql += (parsedCriteria['google_product_category_l2'] ? '"' + parsedCriteria['google_product_category_l2'] + '"' : 'NULL') + ',';
    sql += (parsedCriteria['google_product_category_l3'] ? '"' + parsedCriteria['google_product_category_l3'] + '"' : 'NULL') + ',';
    sql += (parsedCriteria['google_product_category_l4'] ? '"' + parsedCriteria['google_product_category_l4'] + '"' : 'NULL') + ',';
    sql += (parsedCriteria['google_product_category_l5'] ? '"' + parsedCriteria['google_product_category_l5'] + '"' : 'NULL') + ',';
    sql += (parsedCriteria['brand'] ? '"' + parsedCriteria['brand'] + '"' : 'NULL') + ',';
    sql += (parsedCriteria['offer_id'] ? '"' + parsedCriteria['offer_id'] + '"' : 'NULL') + ',';
    sql += (parsedCriteria['channel'] ? '"' + parsedCriteria['channel'] + '"' : 'NULL') + ',';
    sql += (parsedCriteria['channel_exclusivity'] ? '"' + parsedCriteria['channel_exclusivity'] + '"' : 'NULL') + ',';
    sql += (parsedCriteria['condition'] ? '"' + parsedCriteria['condition'] + '"' : 'NULL');
    sql += ')';
    i += 1;
  }}
  return sql;
  """;
