"""Investor profile: objectives, horizon, risk tolerance (FC-C3, FC-C4, FC-P1).

This encodes the *assess* step (FC-P1): before any allocation, the investor's
objectives, horizon, risk tolerance, and liquidity constraints are captured as
domain state that later phases (allocation, monitoring) consume.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal

from pmcore.domain.enums import InvestmentHorizon, ObjectiveType, RiskProfile
from pmcore.domain.instrument import Benchmark
from pmcore.domain.money import Currency, Money


@dataclass(frozen=True, slots=True)
class Objective:
    """A single investment goal (FC-C3).

    Examples: retirement in 25y, emergency fund of 6 months expenses.
    `target_amount` is optional for open-ended goals (e.g. wealth accumulation).
    """

    objective_type: ObjectiveType
    horizon: InvestmentHorizon
    target_amount: Money | None = None
    note: str = ""

    def __post_init__(self) -> None:
        if self.target_amount is not None and self.target_amount.amount < 0:
            raise ValueError("target amount cannot be negative")


@dataclass(frozen=True, slots=True)
class Constraints:
    """Liquidity / flexibility constraints (FC-P1 assess step)."""

    min_cash_weight: Decimal = Decimal("0.00")
    max_single_position_weight: Decimal = Decimal("1.00")
    liquidity_note: str = ""

    def __post_init__(self) -> None:
        if not (Decimal("0.00") <= self.min_cash_weight <= Decimal("1.00")):
            raise ValueError("min_cash_weight must be within [0, 1]")
        if not (Decimal("0.00") < self.max_single_position_weight <= Decimal("1.00")):
            raise ValueError("max_single_position_weight must be within (0, 1]")


@dataclass(frozen=True, slots=True)
class InvestorProfile:
    """The assessed investor situation (FC-P1) driving allocation (FC-P2).

    Suggested profile ↔ benchmark defaults are out of scope here; the
    allocation layer (Phase 4) decides mixes per RiskProfile (FC-C4).
    """

    display_name: str
    risk_profile: RiskProfile
    base_currency: Currency
    objectives: tuple[Objective, ...] = ()
    constraints: Constraints = field(default_factory=Constraints)
    benchmark: Benchmark | None = None  # FC-M1

    def __post_init__(self) -> None:
        if not self.objectives:
            raise ValueError("an investor profile requires at least one objective (FC-C3)")

    def progress_towards(self, objective: Objective, portfolio_value: Money) -> Decimal:
        """Fraction of an objective's target met (FC-C6; 0 if open-ended)."""
        if objective.target_amount is None or objective.target_amount.amount == 0:
            return Decimal(0)
        if portfolio_value.currency is not objective.target_amount.currency:
            raise ValueError("portfolio value must be converted to the objective currency first")
        return portfolio_value.amount / objective.target_amount.amount
