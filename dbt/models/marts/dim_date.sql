-- Dimension: a date spine covering the full order history.
-- A proper date dimension lets every fact join to consistent calendar
-- attributes (weekday, month, quarter) without recomputing them per query.
with bounds as (
    select
        least(
            (select min(order_date) from {{ ref('int_shopify_deduplicated') }}),
            (select min(txn_date)   from {{ ref('int_pos_classified') }})
        ) as start_date,
        greatest(
            (select max(settlement_date) from {{ ref('int_amazon_normalized') }}),
            (select max(txn_date)        from {{ ref('int_pos_classified') }})
        ) as end_date
),
spine as (
    select generate_series(
        (select start_date from bounds),
        (select end_date from bounds),
        interval '1 day'
    )::date as date_day
)
select
    date_day                                     as date_key,
    extract(year  from date_day)::int            as year,
    extract(month from date_day)::int            as month,
    extract(day   from date_day)::int            as day,
    extract(quarter from date_day)::int          as quarter,
    extract(dow   from date_day)::int            as day_of_week,
    to_char(date_day, 'Day')                     as day_name,
    (extract(dow from date_day) in (0, 6))       as is_weekend
from spine
