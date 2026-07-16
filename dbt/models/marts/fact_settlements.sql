-- Fact: Amazon settlements. Grain: one row per settled line. This is the CASH
-- view for Amazon - revenue recognised on the settlement date, net of fees,
-- currency-normalized. It is deliberately separate from fact_order_lines because
-- Amazon money lands ~14 days after the order in a different amount. Keeping the
-- two facts distinct is what lets each reconcile to the bank on its own basis.
select
    row_number() over (order by settlement_date) as settlement_key,
    order_id,
    settlement_date as activity_date,
    channel,
    sku as product_key,
    quantity,
    gross_amount_usd,
    fee_amount_usd,
    net_amount_usd,
    original_currency
from {{ ref('int_amazon_normalized') }}
