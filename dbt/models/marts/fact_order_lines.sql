-- Fact: every order line across the two order-date channels (Shopify + POS).
-- Grain: one row per sold line. This is the "what was ordered, when, where"
-- fact - order-date revenue, the operational view. Amazon is a SEPARATE fact
-- because its money is recognised at settlement, not order date (see
-- fact_settlements). Mixing them would double-count or misdate revenue.
with shopify as (
    select
        order_id, order_date as activity_date, channel,
        null::text as store_id, sku, quantity,
        round(quantity * unit_price, 2) as gross_revenue
    from {{ ref('int_shopify_deduplicated') }}
),
pos as (
    select
        null::bigint as order_id, txn_date as activity_date, channel,
        store_id, sku, quantity,
        amount as gross_revenue
    from {{ ref('int_pos_classified') }}
)
select
    row_number() over (order by activity_date) as order_line_key,
    activity_date,
    channel,
    store_id,
    sku as product_key,
    quantity,
    gross_revenue
from (
    select * from shopify
    union all
    select * from pos
) combined
