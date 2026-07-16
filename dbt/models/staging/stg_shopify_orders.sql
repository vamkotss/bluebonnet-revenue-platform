-- Staging: standardize raw Shopify rows. Cast types, normalize the order id,
-- split orders from refunds. NO dedupe yet - that is an intermediate concern,
-- kept separate so the staging layer stays a clean 1:1 view of raw.
with source as (
    select * from {{ source('raw', 'shopify_orders') }}
)
select
    -- Strip the "SHOP-" prefix to a clean integer order id for joining.
    replace(order_id, 'SHOP-', '')::bigint  as order_id,
    created_at::timestamptz                 as order_ts,
    created_at::date                        as order_date,
    sku,
    quantity::int                           as quantity,
    price::numeric(12,2)                     as unit_price,
    record_type,
    refund_amount::numeric(12,2)            as refund_amount,
    source_file
from source
