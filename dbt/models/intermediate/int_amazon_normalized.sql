-- Intermediate: normalize Amazon currency and expose order-date revenue.
-- RULING: CAD rows are reported in Canadian dollars and must be converted to USD
-- before summing, or the total overcounts. We apply a fixed reference rate
-- (documented as an assumption). USD rows pass through unchanged.
with settlements as (
    select * from {{ ref('stg_amazon_settlements') }}
),
converted as (
    select *,
        -- Fixed reference FX rate. In production this joins a rates table; here
        -- it is a stated constant so the conversion is explicit and testable.
        case when currency = 'CAD' then 0.73 else 1.0 end as fx_to_usd
    from settlements
)
select
    order_id, sku, quantity,
    round(gross_amount * fx_to_usd, 2) as gross_amount_usd,
    round(fee_amount   * fx_to_usd, 2) as fee_amount_usd,
    round(net_amount   * fx_to_usd, 2) as net_amount_usd,
    currency as original_currency,
    settlement_date,
    'amazon' as channel
from converted
