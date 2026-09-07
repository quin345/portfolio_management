"""Value objects for money and FX conversion (FC-C5, FC-C6).

Pure Python / Decimal — no vendor or broker imports. Currency conversion
requires an explicit FxConverter supplied by the caller (the data layer,
from Phase 2, is the source of truth — TECHNICAL.md §3.3).
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_EVEN
from enum import Enum
from typing import Protocol


class Currency(str, Enum):
    """Supported settlement currencies (extensible)."""

    USD = "USD"
    EUR = "EUR"
    GBP = "GBP"
    CHF = "CHF"
    JPY = "JPY"


@dataclass(frozen=True, slots=True)
class Money:
    """An amount in a single currency. Invariant: same-currency arithmetic only."""

    amount: Decimal
    currency: Currency

    def __post_init__(self) -> None:
        if not isinstance(self.amount, Decimal):
            raise TypeError("Money.amount must be a Decimal")

    # -- construction helpers -------------------------------------------------
    @staticmethod
    def of(amount: str | int | Decimal, currency: Currency) -> "Money":
        return Money(Decimal(amount), currency)

    @staticmethod
    def zero(currency: Currency) -> "Money":
        return Money(Decimal(0), currency)

    # -- same-currency arithmetic --------------------------------------------
    def _check(self, other: "Money") -> None:
        if not isinstance(other, Money):
            raise TypeError(f"cannot combine Money with {type(other).__name__}")
        if self.currency is not other.currency:
            raise ValueError(
                f"currency mismatch: {self.currency} vs {other.currency}; "
                "convert explicitly via FxConverter"
            )

    def __add__(self, other: "Money") -> "Money":
        self._check(other)
        return Money(self.amount + other.amount, self.currency)

    def __sub__(self, other: "Money") -> "Money":
        self._check(other)
        return Money(self.amount - other.amount, self.currency)

    def __mul__(self, factor: int | Decimal) -> "Money":
        return Money(self.amount * Decimal(factor), self.currency)

    def __neg__(self) -> "Money":
        return Money(-self.amount, self.currency)

    def scaled(self, weight: Decimal, quantum: str = "0.0001") -> "Money":
        """Scale by a portfolio weight, rounded to a fixed quantum (deterministic)."""
        return Money(
            (self.amount * weight).quantize(Decimal(quantum), ROUND_HALF_EVEN),
            self.currency,
        )


@dataclass(frozen=True, slots=True)
class FxRate:
    """A quoted rate: 1 unit of `base` = `rate` units of `quote`."""

    base: Currency
    quote: Currency
    rate: Decimal

    def __post_init__(self) -> None:
        if self.base is self.quote and self.rate != Decimal(1):
            raise ValueError("same-currency fx rate must be the identity (1)")
        if self.base is not self.quote and self.rate <= 0:
            raise ValueError("fx rate must be positive")
        if not isinstance(self.rate, Decimal):
            raise TypeError("FxRate.rate must be a Decimal")


class FxConverter(Protocol):
    """Minimal conversion port — the data layer provides the implementation."""

    def rate(self, base: Currency, quote: Currency) -> FxRate: ...


class RateTable:
    """Simple in-memory FxConverter built from quoted pairs (both directions)."""

    def __init__(self, rates: list[FxRate]) -> None:
        self._rates: dict[tuple[Currency, Currency], FxRate] = {}
        for r in rates:
            self._rates[(r.base, r.quote)] = r
            self._rates[(r.quote, r.base)] = FxRate(r.quote, r.base, Decimal(1) / r.rate)

    def rate(self, base: Currency, quote: Currency) -> FxRate:
        if base is quote:
            return FxRate(base, quote, Decimal(1))
        try:
            return self._rates[(base, quote)]
        except KeyError:
            raise KeyError(f"no fx rate for {base}->{quote}") from None

    def convert(self, money: Money, to: Currency) -> Money:
        r = self.rate(money.currency, to)
        return Money(money.amount * r.rate, to)
