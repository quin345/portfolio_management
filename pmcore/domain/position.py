"""Position — holding of one instrument, with mark-to-market (FC-C5, FC-C6)."""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal

from pmcore.domain.instrument import Instrument
from pmcore.domain.money import Money
from pmcore.domain.transaction import Transaction, TransactionType


@dataclass
class Position:
    """A position in a single instrument.

    Invariants: quantity may be >= 0 (short support deferred by design —
    active discretionary long-only baseline; revisit via FC traceability).
    avg_cost is in the instrument currency.
    """

    instrument: Instrument
    quantity: Decimal = Decimal(0)
    avg_cost: Money | None = None
    _mark: Money | None = field(default=None, repr=False)

    def __post_init__(self) -> None:
        if self.quantity < 0:
            raise ValueError("negative quantities (shorts) are not supported yet")
        if self.avg_cost is not None and self.avg_cost.currency is not self.instrument.currency:
            raise ValueError("avg_cost must be in the instrument currency")
        if self._mark is not None and self._mark.currency is not self.instrument.currency:
            raise ValueError("mark must be in the instrument currency")

    # -- marking --------------------------------------------------------------
    def mark(self, price: Money) -> None:
        if price.currency is not self.instrument.currency:
            raise ValueError("mark must be in the instrument currency")
        if price.amount <= 0:
            raise ValueError("mark price must be positive")
        self._mark = price

    def market_value(self) -> Money:
        if self.quantity == 0:
            return Money.zero(self.instrument.currency)
        if self._mark is None:
            raise ValueError(f"position {self.instrument.symbol} has no mark price")
        return Money(self.quantity * self._mark.amount, self.instrument.currency)

    def unrealized_pnl(self) -> Money:
        """Mark-to-market PnL vs average cost (FC-C6)."""
        if self.quantity == 0 or self.avg_cost is None or self._mark is None:
            return Money.zero(self.instrument.currency)
        return Money((self._mark.amount - self.avg_cost.amount) * self.quantity, self.instrument.currency)

    # -- mutation via transactions ---------------------------------------------
    def apply(self, tx: Transaction) -> None:
        """Apply a BUY/SELL to this position, maintaining weighted-average cost."""
        if tx.tx_type not in (TransactionType.BUY, TransactionType.SELL):
            raise ValueError(f"cash transaction {tx.tx_type} cannot be applied to a position")
        if tx.instrument is None or tx.instrument.symbol != self.instrument.symbol:
            raise ValueError("transaction applies to a different instrument")
        if tx.tx_type is TransactionType.BUY:
            assert tx.quantity is not None and tx.price is not None
            cost = tx.price.amount * tx.quantity
            if self.quantity == 0 or self.avg_cost is None:
                self.avg_cost = Money(cost / tx.quantity, self.instrument.currency)
            else:
                total_qty = self.quantity + tx.quantity
                self.avg_cost = Money(
                    (self.avg_cost.amount * self.quantity + cost) / total_qty,
                    self.instrument.currency,
                )
            self.quantity += tx.quantity
        elif tx.tx_type is TransactionType.SELL:
            assert tx.quantity is not None
            if tx.quantity > self.quantity:
                raise ValueError(
                    f"cannot sell {tx.quantity} of {self.instrument.symbol}; holding {self.quantity}"
                )
            self.quantity -= tx.quantity
            if self.quantity == 0:
                self.avg_cost = None
        else:
            raise ValueError(f"cash transaction {tx.tx_type} cannot be applied to a position")
