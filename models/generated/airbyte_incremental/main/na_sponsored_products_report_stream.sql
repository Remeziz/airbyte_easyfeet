{{ config(
    unique_key = '_airbyte_ab_id',
    schema = "main",
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('na_sponsored_products_report_stream_ab3') }}
SELECT
 STR_TO_DATE(reportdate, '%Y%m%d') as date,
        recordtype,
        replace(json_extract(metric, '$."sku"') , '"', '') as sku,
        replace(json_extract(metric, '$."asin"') , '"', '') as asin,
        sum(replace(json_extract(metric, '$."cost"') , '"', '')) as cost,
        sum(replace(json_extract(metric, '$."impressions"') , '"', '')) as impressions,
        sum(replace(json_extract(metric, '$."clicks"') , '"', '')) as clicks,
        sum(replace(json_extract(metric, '$."attributedSales1d"') , '"', '')) as sales,
        sum(replace(json_extract(metric, '$."attributedUnitsOrdered1d"') , '"', '')) as orders
from {{ ref('na_sponsored_products_report_stream_ab3') }}
-- na_sponsored_products_report_stream from {{ source('main', '_airbyte_raw_na_spons__roducts_report_stream') }}
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

