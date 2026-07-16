-- Staging: POS transactions. Preserve negative quantity and txn_type exactly -
-- the returns-vs-training decision is made in intermediate, where it is
-- documented. Here we only cast and derive a clean date.
with source as (
    select * from {{ source('raw', 'pos_transactions') }}
)
select
    store_id,
    txn_timestamp::timestamptz  as txn_ts,
    txn_timestamp::date         as txn_date,
    sku,
    quantity::int               as quantity,
    amount::numeric(12,2)       as amount,
    txn_type,
    source_file
from source
