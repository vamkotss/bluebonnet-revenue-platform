-- Custom reconciliation test: Shopify warehouse revenue must tie to the bank
-- within tolerance. Shopify is the channel with complete source data, so it is
-- the one that MUST reconcile - a failure here means the pipeline broke, not that
-- data is missing. Returns rows = test fails.
select channel, gap, gap_pct, status
from {{ ref('fct_revenue_reconciliation') }}
where channel = 'shopify'
  and status != 'RECONCILED'
