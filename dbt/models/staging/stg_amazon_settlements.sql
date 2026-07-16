-- Staging: Amazon settlements. Both source schemas were already unified at load
-- into a common raw shape, so here we just cast and tag. Currency is preserved
-- (not yet converted) - the CAD normalization is a documented intermediate step.
with source as (
    select * from {{ source('raw', 'amazon_settlements') }}
)
select
    replace(amazon_order_id, 'AMZ-', '')::bigint as order_id,
    sku,
    quantity::int                    as quantity,
    gross_amount::numeric(12,2)      as gross_amount,
    fee_amount::numeric(12,2)        as fee_amount,
    net_amount::numeric(12,2)        as net_amount,
    upper(coalesce(currency, 'USD')) as currency,
    settlement_date::date            as settlement_date,
    source_file
from source
