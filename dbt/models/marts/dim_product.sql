-- Dimension: the resolved product catalogue, one row per SKU.
select
    sku          as product_key,
    product_name,
    category,
    unit_cost,
    unit_price,
    round(unit_price - unit_cost, 2)                        as unit_margin,
    round((unit_price - unit_cost) / nullif(unit_price,0), 3) as margin_pct
from {{ ref('int_products_resolved') }}
