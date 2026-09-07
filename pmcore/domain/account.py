"""Account — one settlement venue's positions + cash (FC-C5).

Broker-agnostic: an Account is identified by a human-chosen name and an
optional, opaque `venue` label (e.g. "IBKR", "MT5:acg", "manual"). The domain
never imports anything venue-specific (TECHNICAL.md §6).
"""

from __future__ import annotations

from dataclasses import dataclass, field

from pmcore.domain.instrument import Instrument
from pmcore.domain.money import Currency, Money
from pmcore.domain.position import Position
from pmcore.domain.transaction import Transaction, TransactionType


@dataclass
class Account:
    name: str
    base_currency: Currency
    venue: str = ""  # opaque label; empty = manual/self-custody
    positions: dict[str, Position] = field(default_factory=dict)
    cash: Money = field(init=False)

    def __post_init__(self) -> None:
        self.cash = Money.zero(self.base_currency)

    # -- position access -------------------------------------------------------
    def position(self, instrument: Instrument) -> Position:
        pos = self.positions.get(instrument.symbol)
        if pos is None:
            pos = Position(instrument)
            self.positions[instrument.symbol] = pos
        return pos

    # -- applying transactions --------------------------------------------------
    def apply(self, tx: Transaction) -> None:
        """Apply a transaction to this account, maintaining cash and positions."""
        if tx.tx_type in (TransactionType.BUY, TransactionType.SELL):
            assert tx.instrument is not None and tx.price is not None and tx.quantity is not None
            if tx.price.currency is not tx.instrument.currency:
                raise ValueError("transaction price must be in instrument currency")
            gross = Money(tx.price.amount * tx.quantity, tx.instrument.currency)
            fee = tx.fee or Money.zero(tx.instrument.currency)
            net = (gross + fee) if tx.tx_type is TransactionType.BUY else (gross - fee)
            # NOTE: cash is held in account base currency; FX conversion is the
            # engine's job (Phase 3) — for now instrument currency must match.
            if net.currency is not self.base_currency:
                raise ValueError(
                    f"cash movement in {net.currency} cannot settle in "
                    f"{self.base_currency} account without FX conversion (Phase 3)"
                )
            self.cash = (self.cash - net) if tx.tx_type is TransactionType.BUY else (self.cash + net)
            self.position(tx.instrument).apply(tx)
        elif tx.tx_type is TransactionType.DEPOSIT:
            assert tx.amount is not None
            self._settle_cash(tx.amount)
        elif tx.tx_type is TransactionType.WITHDRAWAL:
            assert tx.amount is not None
            self._settle_cash(-tx.amount)
        elif tx.tx_type in (TransactionType.FEE,):
            assert tx.amount is not None
            self._settle_cash(-tx.amount)
        else:  # DIVIDEND / INTEREST
            assert tx.amount is not None
            self._settle_cash(tx.amount)

    def _settle_cash(self, delta: Money) -> None:
        if delta.currency is not self.base_currency:
            raise ValueError(f"cash movement must be in {self.base_currency}")
        self.cash = self.cash + delta
