"""Transactions — the record of portfolio events (FC-C5, FC-F1).

Broker-agnostic: a Transaction is produced either by a user record, an
ExecutionVenue adapter (FC-D7), or the data layer. The domain only knows the
event vocabulary.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from enum import Enum

from pmcore.domain.instrument import Instrument
from pmcore.domain.money import Money


class TransactionType(str, Enum):
    BUY = "BUY"
    SELL = "SELL"
    DEPOSIT = "DEPOSIT"        # cash in
    WITHDRAWAL = "WITHDRAWAL"  # cash out
    FEE = "FEE"                # FC-F1: fees are first-class for cost awareness
    DIVIDEND = "DIVIDEND"
    INTEREST = "INTEREST"


@dataclass(frozen=True, slots=True)
class Transaction:
    """An immutable portfolio event.

    For BUY/SELL: quantity > 0, price in the instrument currency, fee optional.
    For DEPOSIT/WITHDRAWAL/FEE/DIVIDEND/INTEREST: `amount` is a cash movement.
    """

    tx_type: TransactionType
    booked_at: datetime | date
    quantity: Decimal | None = None
    price: Money | None = None
    fee: Money | None = None
    amount: Money | None = None  # cash movement (DEPOSIT/WITHDRAWAL/...)
    instrument: Instrument | None = None

    def __post_init__(self) -> None:
        if self.tx_type in (TransactionType.BUY, TransactionType.SELL):
            if self.instrument is None:
                raise ValueError(f"{self.tx_type} requires an instrument")
            if self.quantity is None or self.quantity <= 0:
                raise ValueError(f"{self.tx_type} requires positive quantity")
            if self.price is None or self.price.amount <= 0:
                raise ValueError(f"{self.tx_type} requires a positive price")
        else:
            if self.amount is None:
                raise ValueError(f"{self.tx_type} requires a cash amount")
