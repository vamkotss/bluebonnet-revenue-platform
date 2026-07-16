-- Staging: product master. Normalize the SKU format drift (BLB -> BB-) so the
-- two SKU schemes join. Duplicate-SKU resolution is an intermediate concern.
with source as (
    select * from {{ source('raw', 'product_master') }}
)
select
    -- Normalize the 2025 format change: BLBDEC-0001 back to BB-DEC-0001.
    case
        when sku like 'BLB%' then 'BB-' || substring(sku, 4, 3) || '-' || split_part(sku, '-', 2)
        else sku
    end                         as sku_normalized,
    sku                         as sku_raw,
    product_name,
    category,
    unit_cost::numeric(12,2)    as unit_cost,
    unit_price::numeric(12,2)   as unit_price,
    source_file
from source
