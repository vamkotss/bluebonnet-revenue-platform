-- Intermediate: classify POS rows and drop training noise.
-- RULING: a negative quantity means a RETURN only when txn_type = 'sale'.
-- txn_type = 'training' rows are practice transactions, not real money - they
-- are excluded entirely. This is the returns-vs-training trap: conflating them
-- would corrupt both the sales and the refund totals.
with pos as (
    select * from {{ ref('stg_pos_transactions') }}
    where txn_type = 'sale'          -- drop training rows entirely
)
select
    store_id,
    txn_ts, txn_date,
    sku,
    quantity,
    amount,
    case when quantity < 0 then 'return' else 'sale' end as line_type,
    'pos' as channel
from pos
