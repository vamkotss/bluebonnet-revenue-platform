-- Reconciliation mart: warehouse net revenue by channel vs the bank control.
-- This is THE audit model - it answers "does our warehouse agree with what
-- actually hit the bank, and if not, by how much and why?"
--
-- Each channel reaches net revenue by a different path, which is the entire
-- reconciliation challenge:
--   shopify : order-date gross MINUS separately-arriving refund objects
--   pos     : sale rows MINUS return rows (returns already negative in the data)
--   amazon  : settlement gross net-of-fee (refunds settle in a LATER period the
--             source window does not fully capture - a documented timing residual)
--
-- The `status` column classifies each channel: RECONCILED within tolerance, or
-- RESIDUAL with a known cause. An auditor reads this one table and knows exactly
-- where the books stand.

{% set tolerance = 0.005 %}   -- 0.5% - tight enough to catch real breakage

with shopify_gross as (
    select 'shopify' as channel,
           sum(quantity * unit_price) as gross_revenue
    from {{ ref('int_shopify_deduplicated') }}
),
shopify_refunds as (
    select 'shopify' as channel,
           coalesce(sum(refund_amount), 0) as refunds
    from {{ ref('stg_shopify_orders') }}
    where record_type = 'refund'
),
shopify_net as (
    select g.channel,
           (g.gross_revenue - r.refunds)::numeric(14,2) as warehouse_net_revenue
    from shopify_gross g join shopify_refunds r using (channel)
),
pos_net as (
    select 'pos' as channel,
           sum(amount)::numeric(14,2) as warehouse_net_revenue
    from {{ ref('int_pos_classified') }}
),
amazon_net as (
    select 'amazon' as channel,
           sum(net_amount_usd)::numeric(14,2) as warehouse_net_revenue
    from {{ ref('int_amazon_normalized') }}
),
combined as (
    select * from shopify_net
    union all select * from pos_net
    union all select * from amazon_net
)

select
    c.channel,
    c.warehouse_net_revenue,
    b.bank_net_revenue,
    (c.warehouse_net_revenue - b.bank_net_revenue)::numeric(14,2) as gap,
    round((abs(c.warehouse_net_revenue - b.bank_net_revenue)
          / nullif(b.bank_net_revenue, 0))::numeric, 4) as gap_pct,
    case
        when abs(c.warehouse_net_revenue - b.bank_net_revenue)
             / nullif(b.bank_net_revenue, 0) <= {{ tolerance }}
        then 'RECONCILED'
        else 'RESIDUAL'
    end as status,
    case
        when c.channel = 'amazon'
             and abs(c.warehouse_net_revenue - b.bank_net_revenue)
                 / nullif(b.bank_net_revenue, 0) > {{ tolerance }}
        then 'Amazon refunds settle in a later period not captured in this window'
        when abs(c.warehouse_net_revenue - b.bank_net_revenue)
             / nullif(b.bank_net_revenue, 0) > {{ tolerance }}
        then 'Refund-date vs order-date timing difference'
        else null
    end as residual_reason
from combined c
join {{ ref('bank_deposits_by_channel') }} b using (channel)
order by channel
