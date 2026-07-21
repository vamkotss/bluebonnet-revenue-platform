"""Ground truth: the orders that really happened.

WHY GROUND TRUTH EXISTS
-----------------------
Every one of Bluebonnet's four data sources is a DEGRADED VIEW of the same
underlying reality: real orders, placed by real customers, for real products.
Shopify records them one way and drops some. Amazon batches them, nets out fees,
and delays them two weeks. POS files arrive nightly, sometimes corrupted,
sometimes not at all.

The pipeline's job is to reconstruct that reality from the wreckage. To PROVE it
did, we need to know what reality was - so we generate it first, cleanly, and
keep it as the answer key. The synthetic "bank deposit" feed that the whole
platform reconciles against is computed from this ground truth.

This module is the only place the truth is clean. Everything downstream of it is
supposed to be a mess.

THE THREE CHANNELS
------------------
  shopify   Direct-to-consumer web store. Immediate, card-settled.
  amazon    Marketplace. Amazon collects, nets its fees, pays out in batches.
  pos       12 physical stores. Cash and card, reconciled nightly.

Each channel has different economics, which is exactly why the three reports
never agree and why a single trusted number is hard.
"""

from __future__ import annotations

from datetime import date, timedelta

import numpy as np
import pandas as pd

import os

SEED = 20260716

# Scale is env-configurable so CI can run a short window fast while local/full
# runs cover 18 months. EVERY defect is still present at CI scale - the messiness
# is proportional, not removed - so CI genuinely exercises the same code paths.
# BB_CI_MODE=1 shrinks the window to ~2 months.
_CI = os.environ.get("BB_CI_MODE") == "1"

START_DATE = date(2024, 7, 1)
END_DATE = date(2024, 8, 31) if _CI else date(2025, 12, 31)   # 2 months in CI, else 18

CHANNELS = ["shopify", "amazon", "pos"]
CHANNEL_WEIGHTS = [0.45, 0.35, 0.20]   # share of order volume

STORES = [f"store_{i:02d}" for i in range(1, 13)]   # 12 physical stores

# Amazon's cut. Netted out of settlements, which is a big reason Amazon revenue
# reported to the bank never matches order-value revenue.
AMAZON_FEE_RATE = 0.15

# Roughly how many orders a day across all channels.
ORDERS_PER_DAY = 120


def _rng(seed: int) -> np.random.Generator:
    return np.random.default_rng(seed)


def build_products(rng: np.random.Generator) -> pd.DataFrame:
    """The catalogue. Clean here; the Excel master downstream mangles it."""
    categories = ["Bedding", "Kitchen", "Decor", "Bath", "Outdoor", "Lighting"]
    n = 200

    rows = []
    for i in range(1, n + 1):
        cat = str(rng.choice(categories))
        # Cost and price. Margin is real and varies by category.
        cost = round(float(rng.uniform(4, 60)), 2)
        price = round(cost * float(rng.uniform(1.8, 3.2)), 2)
        rows.append(
            {
                "sku": f"BB-{cat[:3].upper()}-{i:04d}",
                "product_name": f"{cat} item {i}",
                "category": cat,
                "unit_cost": cost,
                "unit_price": price,
            }
        )

    return pd.DataFrame(rows)


def build_orders(products: pd.DataFrame, rng: np.random.Generator) -> pd.DataFrame:
    """Every order line that really happened, across all channels. The answer key.

    One row per order line. An order can have several lines. This is the clean
    truth: correct amounts, correct dates, correct channel, no duplicates, no
    drift, no encoding damage. The mess is added later, per channel.
    """
    product_records = products.to_dict("records")
    days = (END_DATE - START_DATE).days

    rows = []
    order_id = 1
    line_id = 1

    for day_offset in range(days + 1):
        order_date = START_DATE + timedelta(days=day_offset)

        # Weekend and seasonal lift - gives the series realistic shape.
        weekday = order_date.weekday()
        season = 1.0 + 0.3 * np.sin((day_offset / 365) * 2 * np.pi)  # yearly cycle
        weekend = 1.25 if weekday >= 5 else 1.0
        n_orders = int(rng.poisson(ORDERS_PER_DAY * season * weekend))

        for _ in range(n_orders):
            channel = str(rng.choice(CHANNELS, p=CHANNEL_WEIGHTS))
            store = str(rng.choice(STORES)) if channel == "pos" else None

            n_lines = int(rng.integers(1, 5))
            this_order = order_id
            order_id += 1

            for _ in range(n_lines):
                product = product_records[int(rng.integers(0, len(product_records)))]
                qty = int(rng.integers(1, 4))
                gross = round(product["unit_price"] * qty, 2)

                rows.append(
                    {
                        "line_id": line_id,
                        "order_id": this_order,
                        "channel": channel,
                        "store_id": store,
                        "order_date": order_date,
                        "sku": product["sku"],
                        "quantity": qty,
                        "unit_price": product["unit_price"],
                        "gross_revenue": gross,
                    }
                )
                line_id += 1

    orders = pd.DataFrame(rows)

    # --- Refunds. THE hard part of revenue reconciliation. ---
    # A share of order lines get refunded, and crucially the refund lands LATER
    # than the order - sometimes weeks later. This is what makes naive
    # order-date revenue disagree with what the bank actually sees.
    refund_mask = rng.random(len(orders)) < 0.07
    orders["is_refunded"] = refund_mask

    refund_lag = rng.integers(3, 45, len(orders))   # 3 to 45 days later
    orders["refund_date"] = [
        (od + timedelta(days=int(lag))) if r else pd.NaT
        for od, lag, r in zip(orders["order_date"], refund_lag, refund_mask, strict=False)
    ]
    # Some refunds are partial.
    partial = rng.random(len(orders)) < 0.4
    orders["refund_amount"] = np.where(
        refund_mask,
        np.where(partial, (orders["gross_revenue"] * rng.uniform(0.3, 0.7)).round(2),
                 orders["gross_revenue"]),
        0.0,
    )

    return orders


def build_bank_deposits(orders: pd.DataFrame) -> pd.DataFrame:
    """The synthetic bank feed. THE control the whole platform reconciles to.

    This is what actually hit Bluebonnet's bank account, by channel by day. It is
    computed from ground truth, so it is correct by construction - but it is
    correct on a CASH basis, which is deliberately different from the order-value
    basis the channel reports use:

      - Refunds reduce the deposit on the REFUND date, not the order date.
      - Amazon deposits are NET of fees and land on a delayed, batched schedule.

    The pipeline's headline achievement is making the warehouse's revenue tie to
    THIS, to within tolerance, by channel by day. If it ties, the books are
    trustworthy. If it does not, something upstream is wrong.
    """
    rows = []

    # Gross receipts on the order date.
    for r in orders.itertuples(index=False):
        # Money in on the order date.
        deposit = r.gross_revenue
        if r.channel == "amazon":
            deposit *= (1 - AMAZON_FEE_RATE)   # Amazon nets its fee
        rows.append(
            {
                "deposit_date": r.order_date,
                "channel": r.channel,
                "amount": round(deposit, 2),
            }
        )

        # Money out on the refund date (later).
        if r.is_refunded:
            refund = r.refund_amount
            if r.channel == "amazon":
                refund *= (1 - AMAZON_FEE_RATE)
            rows.append(
                {
                    "deposit_date": r.refund_date.date() if hasattr(r.refund_date, "date")
                    else r.refund_date,
                    "channel": r.channel,
                    "amount": round(-refund, 2),
                }
            )

    bank = pd.DataFrame(rows)
    # Aggregate to channel-day: what the bank statement actually shows.
    bank = (
        bank.groupby(["deposit_date", "channel"], as_index=False)["amount"]
        .sum()
        .rename(columns={"amount": "net_deposit"})
    )
    bank["net_deposit"] = bank["net_deposit"].round(2)

    return bank
