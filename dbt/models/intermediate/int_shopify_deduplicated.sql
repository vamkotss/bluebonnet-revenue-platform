-- Intermediate: remove the API-retry duplicate orders.
-- RULING: a Shopify order_id is unique per real order. Retries produced exact
-- duplicate line sets. We keep one copy of each (order_id, sku, unit_price,
-- quantity) line. Using row_number over the natural key is safe because a
-- genuine repeat purchase would be a different order_id.
with orders as (
    select * from {{ ref('stg_shopify_orders') }}
    where record_type = 'order'
),
deduplicated as (
    select *,
        row_number() over (
            partition by order_id, sku, quantity, unit_price
            order by source_file
        ) as rn
    from orders
)
select
    order_id, order_ts, order_date, sku, quantity, unit_price,
    'shopify' as channel
from deduplicated
where rn = 1
