"""Tests for Position and Transaction rules (FC-C5, FC-C6, FC-F1)."""

from datetime import date
from decimal import Decimal

import pytest

from pmcore.domain.enums import AssetClass
from pmcore.domain.instrument import Instrument
from pmcore.domain.money import Currency, Money
from pmcore.domain.position import Position
from pmcore.domain.transaction import Transaction, TransactionType


@pytest.fixture
def aapl() -> Instrument:
    return Instrument("AAPL", AssetClass.EQUITY, Currency.USD, "Apple Inc.")


def test_buy_builds_weighted_average_cost(aapl):
    pos = Position(aapl)
    pos.apply(Transaction(TransactionType.BUY, date(2026, 1, 1), Decimal("10"), Money.of("100", Currency.USD), instrument=aapl))
    pos.apply(Transaction(TransactionType.BUY, date(2026, 1, 2), Decimal("10"), Money.of("120", Currency.USD), instrument=aapl))
    assert pos.quantity == Decimal("20")
    assert pos.avg_cost is not None and pos.avg_cost.amount == Decimal("110")


def test_sell_reduces_and_keeps_avg_cost(aapl):
    pos = Position(aapl)
    pos.apply(Transaction(TransactionType.BUY, date(2026, 1, 1), Decimal("10"), Money.of("100", Currency.USD), instrument=aapl))
    pos.apply(Transaction(TransactionType.SELL, date(2026, 1, 2), Decimal("4"), Money.of("110", Currency.USD), instrument=aapl))
    assert pos.quantity == Decimal("6")
    assert pos.avg_cost is not None and pos.avg_cost.amount == Decimal("100")


def test_sell_more_than_held_raises(aapl):
    pos = Position(aapl)
    pos.apply(Transaction(TransactionType.BUY, date(2026, 1, 1), Decimal("5"), Money.of("100", Currency.USD), instrument=aapl))
    with pytest.raises(ValueError, match="cannot sell"):
        pos.apply(Transaction(TransactionType.SELL, date(2026, 1, 2), Decimal("6"), Money.of("110", Currency.USD), instrument=aapl))


def test_market_value_and_unrealized_pnl(aapl):
    pos = Position(aapl)
    pos.apply(Transaction(TransactionType.BUY, date(2026, 1, 1), Decimal("10"), Money.of("100", Currency.USD), instrument=aapl))
    with pytest.raises(ValueError, match="no mark"):
        pos.market_value()
    pos.mark(Money.of("115", Currency.USD))
    assert pos.market_value().amount == Decimal("1150")
    assert pos.unrealized_pnl().amount == Decimal("150")


def test_mark_validations(aapl):
    pos = Position(aapl)
    with pytest.raises(ValueError, match="instrument currency"):
        pos.mark(Money.of("100", Currency.EUR))
    with pytest.raises(ValueError, match="positive"):
        pos.mark(Money.of("0", Currency.USD))


def test_negative_quantity_rejected(aapl):
    with pytest.raises(ValueError, match="short"):
        Position(aapl, quantity=Decimal("-1"))


def test_cash_transaction_rejected_on_position(aapl):
    pos = Position(aapl)
    with pytest.raises(ValueError, match="cannot be applied"):
        pos.apply(Transaction(TransactionType.DEPOSIT, date(2026, 1, 1), amount=Money.of("100", Currency.USD)))


def test_transaction_validation(aapl):
    with pytest.raises(ValueError, match="instrument"):
        Transaction(TransactionType.BUY, date(2026, 1, 1), Decimal("10"), Money.of("100", Currency.USD))
    with pytest.raises(ValueError, match="positive quantity"):
        Transaction(TransactionType.BUY, date(2026, 1, 1), Decimal("0"), Money.of("100", Currency.USD), instrument=aapl)
    with pytest.raises(ValueError, match="cash amount"):
        Transaction(TransactionType.DEPOSIT, date(2026, 1, 1))