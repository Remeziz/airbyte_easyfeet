
with __dbt__cte__na_sponsored_products_report_stream_ab1 as (

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

),  __dbt__cte__na_sponsored_products_report_stream_ab2 as (

-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: __dbt__cte__na_sponsored_products_report_stream_ab1
select
    cast(metric as json) as metric,
    cast(profileid as 
    signed
) as profileid,
    cast(nullif(updatedat, '') as char(1024)) as updatedat,
    cast(recordtype as char(1024)) as recordtype,
    cast(reportdate as char(1024)) as reportdate,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    
    CURRENT_TIMESTAMP
 as _airbyte_normalized_at
from __dbt__cte__na_sponsored_products_report_stream_ab1
-- na_sponsored_products_report_stream
where 1 = 1

),  __dbt__cte__na_sponsored_products_report_stream_ab3 as (

-- SQL model to build a hash column based on the values of this record
-- depends_on: __dbt__cte__na_sponsored_products_report_stream_ab2
select
    md5(cast(concat(coalesce(cast(metric as char), ''), '-', coalesce(cast(profileid as char), ''), '-', coalesce(cast(updatedat as char), ''), '-', coalesce(cast(recordtype as char), ''), '-', coalesce(cast(reportdate as char), '')) as char)) as _airbyte_na_sponsored___report_stream_hashid,
    tmp.*
from __dbt__cte__na_sponsored_products_report_stream_ab2 tmp
-- na_sponsored_products_report_stream
where 1 = 1

)-- Final base SQL model
-- depends_on: __dbt__cte__na_sponsored_products_report_stream_ab3
select
    metric,
    profileid,
    updatedat,
    recordtype,
    reportdate,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    
    CURRENT_TIMESTAMP
 as _airbyte_normalized_at,
    _airbyte_na_sponsored___report_stream_hashid
from __dbt__cte__na_sponsored_products_report_stream_ab3
-- na_sponsored_products_report_stream from main._airbyte_raw_na_spons__roducts_report_stream
where 1 = 1
