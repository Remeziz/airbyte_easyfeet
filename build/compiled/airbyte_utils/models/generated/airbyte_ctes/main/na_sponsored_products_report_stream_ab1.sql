
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: main._airbyte_raw_na_spons__roducts_report_stream
select
    
        json_extract(table_alias._airbyte_data, 
    '$."metric"')
     as metric,
    json_value(_airbyte_data, 
    '$."profileId"' RETURNING CHAR) as profileid,
    json_value(_airbyte_data, 
    '$."updatedAt"' RETURNING CHAR) as updatedat,
    json_value(_airbyte_data, 
    '$."recordType"' RETURNING CHAR) as recordtype,
    json_value(_airbyte_data, 
    '$."reportDate"' RETURNING CHAR) as reportdate,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    
    CURRENT_TIMESTAMP
 as _airbyte_normalized_at
from main._airbyte_raw_na_spons__roducts_report_stream as table_alias
-- na_sponsored_products_report_stream
where 1 = 1