"""Write the dbt bank-control seed from the current ground-truth answer key.

WHY THIS IS A SCRIPT, NOT A STATIC FILE
---------------------------------------
The reconciliation compares warehouse revenue to a bank control. That control
must reflect the SAME data the warehouse was built from. A static seed baked from
18-month data would not match a 2-month CI run, and reconciliation would fail for
a bookkeeping reason rather than a real one.

So the seed is derived from _bank_deposits.parquet - the answer key the generator
writes - guaranteeing the control always matches the data in play, at any scale.

Run:  python -m generators.make_bank_seed
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd


def make_seed(raw_dir: Path = Path("data/raw"),
              seed_path: Path = Path("dbt/seeds/bank_deposits_by_channel.csv")) -> None:
    bank = pd.read_parquet(raw_dir / "_bank_deposits.parquet")
    by_channel = bank.groupby("channel")["net_deposit"].sum().round(2).reset_index()
    by_channel.columns = ["channel", "bank_net_revenue"]
    seed_path.parent.mkdir(parents=True, exist_ok=True)
    by_channel.to_csv(seed_path, index=False)
    print(f"Bank seed written to {seed_path}:")
    print(by_channel.to_string(index=False))


if __name__ == "__main__":
    make_seed()
