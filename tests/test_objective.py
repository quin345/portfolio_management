"""Tests for InvestorProfile / Objective / Constraints (FC-C3, FC-C4, FC-P1)."""

from decimal import Decimal

import pytest

from pmcore.domain.enums import InvestmentHorizon, ObjectiveType, RiskProfile
from pmcore.domain.money import Currency, Money
from pmcore.domain.objective import Constraints, InvestorProfile, Objective


def _obj(**kw) -> Objective:
    defaults = dict(objective_type=ObjectiveType.RETIREMENT, horizon=InvestmentHorizon.LONG_TERM)
    defaults.update(kw)
    return Objective(**defaults)


def test_profile_requires_objective():
    with pytest.raises(ValueError, match="at least one objective"):
        InvestorProfile("jessi", RiskProfile.MODERATE, Currency.USD, objectives=())


def test_objective_progress():
    o = _obj(target_amount=Money.of("1000000", Currency.USD))
    p = InvestorProfile(
        "jessi", RiskProfile.AGGRESSIVE, Currency.USD, objectives=(o,),
        constraints=Constraints(min_cash_weight=Decimal("0.05")),
    )
    assert p.progress_towards(o, Money.of("250000", Currency.USD)) == Decimal("0.25")


def test_open_ended_objective_progress_is_zero():
    o = _obj()  # no target amount
    p = InvestorProfile("jessi", RiskProfile.MODERATE, Currency.USD, objectives=(o,))
    assert p.progress_towards(o, Money.of("500000", Currency.USD)) == Decimal(0)


def test_progress_currency_mismatch_raises():
    o = _obj(target_amount=Money.of("1000000", Currency.EUR))
    p = InvestorProfile("jessi", RiskProfile.MODERATE, Currency.USD, objectives=(o,))
    with pytest.raises(ValueError, match="converted"):
        p.progress_towards(o, Money.of("500000", Currency.USD))


def test_constraints_validation():
    with pytest.raises(ValueError, match="min_cash_weight"):
        Constraints(min_cash_weight=Decimal("1.5"))
    with pytest.raises(ValueError, match="max_single_position_weight"):
        Constraints(max_single_position_weight=Decimal("0"))
