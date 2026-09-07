"""Tests for Account and Portfolio aggregation (FC-C5) and weight invariants (§3.3)."""

from datetime import date
from decimal import Decimal

import pytest

from pmcore.domain.account import Account
from pmcore.domain.enums import AssetClass
from pmcore.domain.instrument import Benchmark, Instrument
from pmcore.domain.money import Currency, FxRate, Money, RateTable
from pmcore.domain.portfolio import Portfolio
from pmcore.domain.transaction import Transaction, TransactionType


@pytest.fixture
def fx() -> RateTable:
    return RateTable(
        [FxRate(Currency.EUR, Currency.USD, Decimal("1.10")), FxRate(Currency.USD, Currency.CHF, Decimal("0.90"))]
    )


def _spy() -> Instrument:
    return Instrument("SPY", AssetClass.EQUITY, Currency.USD, "S&P 500 ETF")


def _agg() -> Instrument:
    return Instrument("AGG", AssetClass.FIXED_INCOME, Currency.USD, "US Agg Bond ETF")


def _fund_account(name: str, cash: str) -> Account:
    acc = Account(name, Currency.USD)
    acc.apply(Transaction(TransactionType.DEPOSIT, date(2026, 1, 1), amount=Money.of(cash, Currency.USD)))
    return acc


def test_account_cash_and_buy_reduce_cash():
    acc = _fund_account("main", "10000")
    acc.apply(
        Transaction(
            TransactionType.BUY, date(2026, 1, 2), Decimal("10"), Money.of("100", Currency.USD),
            fee=Money.of("1", Currency.USD), instrument=_spy(),
        )
    )
    assert acc.cash.amount == Decimal("8999")  # 10000 - 1000 - 1
    assert acc.positions["SPY"].quantity == Decimal("10")


def test_account_rejects_mismatched_currency():
    acc = Account("eur-acct", Currency.EUR)
    with pytest.raises(ValueError, match="FX conversion"):
        acc.apply(
            Transaction(TransactionType.BUY, date(2026, 1, 1), Decimal("1"), Money.of("100", Currency.USD), instrument=_spy())
        )


def test_portfolio_consolidates_accounts(fx):
    p = Portfolio("core")
    a1 = _fund_account("a1", "5000")
    a2 = _fund_account("a2", "10000")
    spy, agg = _spy(), _agg()
    a1.apply(Transaction(TransactionType.BUY, date(2026, 1, 2), Decimal("10"), Money.of("100", Currency.USD), instrument=spy))
    a2.apply(Transaction(TransactionType.BUY, date(2026, 1, 2), Decimal("50"), Money.of("100", Currency.USD), instrument=agg))
    a1.position(spy).mark(Money.of("105", Currency.USD))
    a2.position(agg).mark(Money.of("100", Currency.USD))
    p.add_account(a1)
    p.add_account(a2)

    total = p.total_value(Currency.USD, fx)
    # a1: 5000 cash - 1000 SPY + 1050 marked = 5050; a2: 10000 - 5000 AGG + 5000 = 10000
    assert total.amount == Decimal("15050")

    exposure = p.exposure_by_asset_class(Currency.USD, fx)
    assert exposure[AssetClass.EQUITY].amount == Decimal("1050")
    assert exposure[AssetClass.FIXED_INCOME].amount == Decimal("5000")
    assert p.cash_value(Currency.USD, fx).amount == Decimal("9000")

    weights = p.weights_by_asset_class(Currency.USD, fx)
    assert abs(sum(weights.values(), Decimal(0)) - Decimal(1)) < Decimal("0.0005")
    assert weights[AssetClass.EQUITY] == Decimal("1050") / Decimal("15050")

def test_portfolio_weight_invariant_holds_with_fx(fx):
    p = Portfolio("multi-ccy")
    usd = _fund_account("usd", "3000")
    eur_acc = Account("eur", Currency.EUR)
    eur_acc.apply(Transaction(TransactionType.DEPOSIT, date(2026, 1, 1), amount=Money.of("2000", Currency.EUR)))
    usd.apply(
        Transaction(TransactionType.BUY, date(2026, 1, 2), Decimal("5"), Money.of("200", Currency.USD), instrument=_spy())
    )
    usd.position(_spy()).mark(Money.of("210", Currency.USD))
    p.add_account(usd)
    p.add_account(eur_acc)

    total = p.total_value(Currency.USD, fx)
    assert total.amount == Decimal("3000") - Decimal("1000") + Decimal("1050") + Decimal("2200")
    weights = p.weights_by_asset_class(Currency.USD, fx)
    assert abs(sum(weights.values(), Decimal(0)) - Decimal(1)) < Decimal("0.0005")


def test_duplicate_account_name_rejected():
    p = Portfolio("x")
    p.add_account(_fund_account("a", "10"))
    with pytest.raises(ValueError, match="duplicate"):
        p.add_account(_fund_account("a", "20"))


def test_unrealized_pnl_consolidated(fx):
    p = Portfolio("pnl")
    a = _fund_account("a", "1000")
    spy = _spy()
    a.apply(Transaction(TransactionType.BUY, date(2026, 1, 2), Decimal("10"), Money.of("100", Currency.USD), instrument=spy))
    a.position(spy).mark(Money.of("103", Currency.USD))
    p.add_account(a)
    assert p.unrealized_pnl(Currency.USD, fx).amount == Decimal("30")


def test_drift_vs_benchmark(fx):
    bench = Benchmark("60/40", asset_class_weights={
        AssetClass.EQUITY: Decimal("0.6"), AssetClass.FIXED_INCOME: Decimal("0.4")
    })
    p = Portfolio("drift", benchmark=bench)
    a = _fund_account("a", "2000")  # exactly funds both buys -> zero cash
    agg = _agg()
    a.apply(Transaction(TransactionType.BUY, date(2026, 1, 2), Decimal("10"), Money.of("100", Currency.USD), instrument=_spy()))
    a.apply(Transaction(TransactionType.BUY, date(2026, 1, 2), Decimal("10"), Money.of("100", Currency.USD), instrument=agg))
    a.position(_spy()).mark(Money.of("250", Currency.USD))   # 2500 equity
    a.position(agg).mark(Money.of("100", Currency.USD))      # 1000 bonds
    p.add_account(a)

    # total 3500: equity 2500 (71.43%), bonds 1000 (28.57%) vs 60/40 target
    drift = p.drift_vs_benchmark(Currency.USD, fx)
    assert abs(drift[AssetClass.EQUITY] - (Decimal("2500") / Decimal("3500") - Decimal("0.6"))) < Decimal("1e-9")
    assert abs(drift[AssetClass.FIXED_INCOME] - (Decimal("1000") / Decimal("3500") - Decimal("0.4"))) < Decimal("1e-9")


def test_drift_requires_benchmark(fx):
    p = Portfolio("no-bench")
    p.add_account(_fund_account("a", "10"))
    with pytest.raises(ValueError, match="benchmark"):
        p.drift_vs_benchmark(Currency.USD, fx)


def test_zero_value_portfolio_weights_rejected(fx):
    p = Portfolio("empty")
    with pytest.raises(ValueError, match="zero-value"):
        p.weights_by_asset_class(Currency.USD, fx)


def test_benchmark_validation():
    with pytest.raises(ValueError, match="sum to 1"):
        Benchmark("bad", asset_class_weights={AssetClass.EQUITY: Decimal("0.5")})
    with pytest.raises(ValueError, match="at least one"):
        Benchmark("empty")