-- Guardrail: no channel's gap may exceed 10%. The known residuals (Amazon
-- refund timing ~4.5%, POS missing files ~1.3%) are documented and tolerated.
-- A gap beyond 10% is not a known residual - it is a pipeline failure, and this
-- test fails loudly so it cannot ship silently.
select channel, gap_pct
from {{ ref('fct_revenue_reconciliation') }}
where gap_pct > 0.10
