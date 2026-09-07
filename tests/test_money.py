"""Tests for Money, FxRate, RateTable — currency handling (FC-C5, FC-C6)."""

from decimal import Decimal

import pytest

from pmcore.domain.money import Currency, FxRate, Money, RateTable


def test_money_same_currency_arithmetic():
    a = Money.of("100.50", Currency.USD)
    b = Money.of("49.50", Currency.USD)
    assert (a + b).amount == Decimal("150.00")
    assert (a - b).amount == Decimal("51.00")
    assert (b * 2).amount == Decimal("99.00")
    assert (-b).amount == Decimal("-49.50")


def test_money_cross_currency_raises():
    usd = Money.of("10", Currency.USD)
    eur = Money.of("10", Currency.EUR)
    with pytest.raises(ValueError, match="currency mismatch"):
        usd + eur


def test_money_requires_decimal():
    with pytest.raises(TypeError, match="Decimal"):
        Money(100.0, Currency.USD)  # float not allowed


def test_fx_rate_validation():
    with pytest.raises(ValueError, match="identity"):
        FxRate(Currency.USD, Currency.USD, Decimal("1.5"))
    # identity rate is allowed
    assert FxRate(Currency.USD, Currency.USD, Decimal(1)).rate == 1
    with pytest.raises(ValueError, match="positive"):
        FxRate(Currency.EUR, Currency.USD, Decimal("0"))
    with pytest.raises(TypeError, match="Decimal"):
        FxRate(Currency.EUR, Currency.USD, 1.1)


def test_rate_table_bidirectional_conversion():
    table = RateTable([FxRate(Currency.EUR, Currency.USD, Decimal("1.10"))])
    eur100 = Money.of("100", Currency.EUR)
    usd = table.convert(eur100, Currency.USD)
    assert usd.currency is Currency.USD
    assert usd.amount == Decimal("110")

    # inverse direction: derived rate 1/1.10
    back = table.convert(usd, Currency.EUR)
    assert back.amount == Decimal("100")

    # same-currency conversion is identity
    assert table.convert(eur100, Currency.EUR) == eur100


def test_rate_table_missing_pair_raises():
    table = RateTable([FxRate(Currency.EUR, Currency.USD, Decimal("1.10"))])
    with pytest.raises(KeyError):
        table.rate(Currency.GBP, Currency.JPY)


def test_scaled_is_deterministic():
    m = Money.of("1", Currency.USD)
    w = Decimal("1") / Decimal("3")
    assert m.scaled(w).amount == Decimal("0.3333")
