"""Portfolio — the aggregate root consolidating accounts (FC-C5, FC-C6).

Domain invariants (TECHNICAL.md §3.3):
- A Portfolio aggregates Positions (via Accounts) into one consolidated view.
- Weight invariants are enforced here: asset-class weights sum to ~1.
- All metrics can be expressed in a single reporting currency via an
  FxConverter; FX is always explicit, never implicit.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal

from pmcore.domain.account import Account
from pmcore.domain.enums import AssetClass
from pmcore.domain.instrument import Benchmark
from pmcore.domain.money import Currency, FxConverter, Money
from pmcore.domain.position import Position

WEIGHT_TOLERANCE = Decimal("0.0005")


@dataclass
class Portfolio:
    """Aggregate root: one or more Accounts consolidated into a single portfolio."""

    name: str
    accounts: list[Account] = field(default_factory=list)
    benchmark: Benchmark | None = None  # FC-M1: metrics are benchmark-aware

    def add_account(self, account: Account) -> None:
        if any(a.name == account.name for a in self.accounts):
            raise ValueError(f"duplicate account name: {account.name}")
        self.accounts.append(account)

    # -- consolidated views -----------------------------------------------------
    def all_positions(self) -> list[tuple[Account, Position]]:
        """Flattened (account, position) pairs; skips zero-quantity positions."""
        return [
            (a, p)
            for a in self.accounts
            for p in a.positions.values()
            if p.quantity != 0
        ]

    def total_value(self, reporting: Currency, fx: FxConverter) -> Money:
        """Total portfolio value (positions + cash) in the reporting currency."""
        total = Money.zero(reporting)
        for account in self.accounts:
            for pos in account.positions.values():
                if pos.quantity != 0:
                    total = total + fx.convert(pos.market_value(), reporting)
            total = total + fx.convert(account.cash, reporting)
        return total

    def exposure_by_asset_class(self, reporting: Currency, fx: FxConverter) -> dict[AssetClass, Money]:
        """Market-value exposure per asset class (FC-C1, FC-C5)."""
        exposure: dict[AssetClass, Money] = {}
        for _account, pos in self.all_positions():
            ac = pos.instrument.asset_class
            value = fx.convert(pos.market_value(), reporting)
            exposure[ac] = exposure.get(ac, Money.zero(reporting)) + value
        return exposure

    def cash_value(self, reporting: Currency, fx: FxConverter) -> Money:
        total = Money.zero(reporting)
        for account in self.accounts:
            total = total + fx.convert(account.cash, reporting)
        return total

    def weights_by_asset_class(self, reporting: Currency, fx: FxConverter) -> dict[AssetClass, Decimal]:
        """Fractional weights per asset class over TOTAL value (incl. cash as CASH).

        Invariant: weights sum to 1 within WEIGHT_TOLERANCE.
        """
        total = self.total_value(reporting, fx)
        if total.amount == 0:
            raise ValueError("cannot compute weights for a zero-value portfolio")
        weights: dict[AssetClass, Decimal] = {}
        for ac, value in self.exposure_by_asset_class(reporting, fx).items():
            weights[ac] = value.amount / total.amount
        cash_w = self.cash_value(reporting, fx).amount / total.amount
        if cash_w:
            weights[AssetClass.CASH] = weights.get(AssetClass.CASH, Decimal(0)) + cash_w
        s = sum(weights.values(), Decimal(0))
        if abs(s - Decimal(1)) > WEIGHT_TOLERANCE:
            raise AssertionError(f"weight invariant violated: sum={s}")  # pragma: no cover
        return weights

    def drift_vs_benchmark(
        self, reporting: Currency, fx: FxConverter
    ) -> dict[AssetClass, Decimal]:
        """Weight deviation from the benchmark per asset class (FC-M1, FC-P3).

        Positive drift = overweight vs benchmark. Requires a benchmark.
        """
        if self.benchmark is None:
            raise ValueError("portfolio has no benchmark; cannot compute drift (FC-M1)")
        weights = self.weights_by_asset_class(reporting, fx)
        classes = set(weights) | set(self.benchmark.asset_class_weights)
        return {
            ac: weights.get(ac, Decimal(0)) - self.benchmark.target_weight_for(ac)
            for ac in classes
        }

    def unrealized_pnl(self, reporting: Currency, fx: FxConverter) -> Money:
        """Consolidated unrealized PnL across all positions (FC-C6)."""
        total = Money.zero(reporting)
        for _account, pos in self.all_positions():
            total = total + fx.convert(pos.unrealized_pnl(), reporting)
        return total
