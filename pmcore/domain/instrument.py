"""Instrument and Benchmark definitions (FC-C2, FC-M1).

FC-M1: performance is always evaluated *against* benchmarks, so Benchmark is a
first-class domain object, not an afterthought.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal

from pmcore.domain.enums import AssetClass
from pmcore.domain.money import Currency


@dataclass(frozen=True, slots=True)
class Instrument:
    """A tradable asset. Vendor-agnostic: identity is (symbol, asset_class)."""

    symbol: str
    asset_class: AssetClass
    currency: Currency
    name: str = ""

    def __post_init__(self) -> None:
        if not self.symbol:
            raise ValueError("symbol must be non-empty")


@dataclass(frozen=True, slots=True)
class Benchmark:
    """A weighting scheme to evaluate the portfolio against (FC-M1, FC-A2).

    Weights are per-asset-class (coarse) or per-symbol (fine); both must each
    sum to ~1 if provided. CASH weight allowed for partial benchmarks.
    """

    name: str
    asset_class_weights: dict[AssetClass, Decimal] = field(default_factory=dict)
    symbol_weights: dict[str, Decimal] = field(default_factory=dict)

    def __post_init__(self) -> None:
        for label, weights in (("asset_class", self.asset_class_weights), ("symbol", self.symbol_weights)):
            if weights:
                total = sum(weights.values(), Decimal(0))
                if abs(total - Decimal(1)) > Decimal("0.001"):
                    raise ValueError(f"benchmark {label} weights must sum to 1 (got {total})")
        if not self.asset_class_weights and not self.symbol_weights:
            raise ValueError("benchmark needs at least one weight scheme")

    def target_weight_for(self, asset_class: AssetClass) -> Decimal:
        """Target weight for an asset class; 0 if the benchmark does not hold it."""
        return self.asset_class_weights.get(asset_class, Decimal(0))
