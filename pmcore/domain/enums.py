"""Domain enumerations: asset classes, risk profiles, horizons (FC-C1, FC-C4, FC-P1)."""

from __future__ import annotations

from enum import Enum


class AssetClass(str, Enum):
    """Top-level asset classes for allocation and diversification (FC-C1, FC-C2)."""

    EQUITY = "EQUITY"
    FIXED_INCOME = "FIXED_INCOME"
    CASH = "CASH"
    REAL_ESTATE = "REAL_ESTATE"
    COMMODITY = "COMMODITY"
    CURRENCY = "CURRENCY"
    CRYPTO = "CRYPTO"


class RiskProfile(str, Enum):
    """Investor risk profiles (FC-C4; source: aggressive/moderate/conservative/income/tax)."""

    AGGRESSIVE = "AGGRESSIVE"
    MODERATE = "MODERATE"
    CONSERVATIVE = "CONSERVATIVE"
    INCOME_ORIENTED = "INCOME_ORIENTED"
    TAX_EFFICIENCY = "TAX_EFFICIENCY"


class InvestmentHorizon(str, Enum):
    """Investment time horizon (FC-P1 assess step)."""

    SHORT_TERM = "SHORT_TERM"      # < 3 years
    MEDIUM_TERM = "MEDIUM_TERM"    # 3-10 years
    LONG_TERM = "LONG_TERM"        # > 10 years


class ObjectiveType(str, Enum):
    """Investment objective categories (FC-C3; source: retirement, wealth, education, emergency)."""

    RETIREMENT = "RETIREMENT"
    WEALTH_ACCUMULATION = "WEALTH_ACCUMULATION"
    EDUCATION = "EDUCATION"
    EMERGENCY_FUND = "EMERGENCY_FUND"
    INCOME = "INCOME"
    CAPITAL_PRESERVATION = "CAPITAL_PRESERVATION"
