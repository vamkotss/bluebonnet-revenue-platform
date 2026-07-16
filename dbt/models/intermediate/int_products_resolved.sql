-- Intermediate: resolve duplicate SKUs to one row per product.
-- RULING: when a SKU appears more than once with conflicting unit costs, keep the
-- LOWEST cost. Rationale: the conflicting entries are data-entry inflations of a
-- true base cost; the lowest is the most defensible floor and never overstates
-- margin. The choice is documented and applied consistently.
with products as (
    select * from {{ ref('stg_product_master') }}
),
ranked as (
    select *,
        row_number() over (
            partition by sku_normalized
            order by unit_cost asc          -- lowest cost wins
        ) as rn
    from products
)
select
    sku_normalized as sku,
    product_name, category,
    unit_cost, unit_price
from ranked
where rn = 1
