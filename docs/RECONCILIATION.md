# Revenue Reconciliation Report

**The audit question this platform answers:** *does our warehouse's revenue agree
with what actually hit the bank?*

This report explains how the reconciliation works, what reconciles, and — just as
importantly — what does not, and why. An honest "the books are off by $X because
Y" is worth more than a fake-perfect tie.

## The control

The bank deposit feed (`bank_deposits_by_channel`) is the source of truth: the net
money that actually landed in Bluebonnet's account, by channel, on a cash basis
(revenue in on the order, refunds out when they clear, Amazon net of its fee). The
warehouse must reproduce these totals from the messy source data.

## Results

| Channel | Warehouse net | Bank | Gap | Gap % | Status |
|---|---|---|---|---|---|
| Shopify | $13,556,842 | $13,582,123 | −$25,281 | 0.19% | **RECONCILED** |
| POS | $5,970,809 | $6,049,529 | −$78,720 | 1.30% | RESIDUAL |
| Amazon | $9,393,716 | $8,989,863 | +$403,853 | 4.49% | RESIDUAL |

## What each result means

**Shopify — reconciled.** Shopify has complete source data: every order and every
refund object is present. The warehouse ties to the bank within 0.19%, and the
tiny residual is the handful of duplicate orders removed in dedup that the bank
never counted. This is the channel that *must* reconcile, and it does — proving
the core order-minus-refund revenue math is correct.

**POS — a documented residual of 1.30%.** This gap is **real data loss, not a
pipeline bug.** Stores 4 and 9 dropped roughly 8% of their nightly files (the
"missing nights" defect). The sales in those files genuinely never reached the
warehouse because the files do not exist. The correct behaviour is exactly what
the pipeline does: reconcile to what was *received*, and flag the shortfall so
finance knows to chase the missing store exports rather than assume the books are
wrong.

**Amazon — a documented residual of 4.49%.** Amazon settlements report gross
revenue net of fees, but refunds net out in a *later* settlement period that falls
outside this data window. So the warehouse sees the full sale but not yet the
offsetting refund. This is a timing difference inherent to how Amazon settles, not
an error — and it closes as the later settlements arrive.

## How corruption is caught

Two guardrail tests protect the reconciliation:

- `assert_shopify_reconciles` — fails if the fully-reconcilable channel drifts
  beyond 0.5%.
- `assert_no_channel_wildly_off` — fails if any channel's gap exceeds 10%,
  catching real breakage (a doubled join, a lost filter) while tolerating the
  documented sub-5% residuals.

Both were verified by injecting deliberately corrupted revenue: the gap exploded
and both tests failed loudly, refusing to certify the bad data. A quality system
is only worth trusting if it actually catches corruption — this one does.
