-- Dimension: the 12 physical stores. Derived from the POS data since there is no
-- separate store master. A real project would source this from an HR/facilities
-- system; here the stores are exactly those that appear in transactions.
select
    store_id                                   as store_key,
    'Store ' || substring(store_id, 7, 2)      as store_name,
    row_number() over (order by store_id)      as store_number
from (
    select distinct store_id
    from {{ ref('int_pos_classified') }}
) s
