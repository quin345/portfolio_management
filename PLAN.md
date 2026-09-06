# PLAN.md — Project Phases

> **Anchor**: Every phase below must trace to a component in `FOUNDATIONAL_COMPONENTS.md` (FC-xx). No phase introduces a feature that cannot be traced there. See `TECHNICAL.md` for how each phase maps onto the codebase.

## Guiding Principles

1. **Broker-agnostic**: the system never depends on a specific broker or execution platform. Brokers, if involved at all, are pluggable adapters behind a uniform interface (FC-D7, FC-C5).
2. **Multi-vendor data**: market data comes from several data vendors (e.g., Dukascopy, vendor APIs, flat files). No single vendor is privileged; all are normalized into one internal schema (FC-C1, FC-C2).
3. **Domain first**: the core domain model (portfolio, position, objective, risk profile) is built before any vendor or broker integration touches it (FC-C3, FC-C4).
4. **Docs as authority**: `FOUNDATIONAL_COMPONENTS.md` is authoritative. `README.md` is reserved for the final phase as front-facing documentation.

---

## Phase 0 — Documentation Foundation ✅ (complete)

- Created `FOUNDATIONAL_COMPONENTS.md` (authoritative domain model, Investopedia-anchored).
- Created `PLAN.md` (this file), `TECHNICAL.md` (wiring), `thoughtProcess.md` (decision log).
- Removed all broker-specific code: `CTraderOpenApiDemo/`, `ibkr_portfolio/`, `mt5_portfolio/`, `prop_firms/`, stale `portfolio_management.sln`.
- Retained `database/` (vendor-agnostic market-data pipeline — seed of the Phase 2 data layer).
- **Trace**: establishes scope against all FC components.

## Phase 1 — Core Domain Model

- Define language-agnostic domain entities: Portfolio, Account, Position, Transaction, AssetClass, Objective, RiskProfile, Benchmark (FC-C3, FC-C4, FC-C6).
- Encode the portfolio-management process loop: assess → allocate → diversify → rebalance → monitor (FC-P1..FC-P4).
- Choose implementation language/stack and project skeleton (single package or layered modules).
- Unit tests for domain rules (aggregation arithmetic, currency handling, weight invariants).
- **Trace**: FC-C3, FC-C4, FC-C6, FC-P1.

## Phase 2 — Multi-Vendor Data Layer

- Define the unified market-data schema (instruments, prices, corporate actions, FX rates).
- Build the vendor adapter interface and implement the first two vendors (migrating/extending the existing `database/data/raw/` pipeline; Dukascopy fetcher is the reference implementation).
- Data quality gates: gap detection, outlier checks, currency consistency.
- **Trace**: FC-C1, FC-C2, FC-P1 (data needed for assessment), "investor must first know... return expectations" (FC-Foundations).

## Phase 3 — Portfolio Engine & Analytics

- Aggregation of positions across accounts into a consolidated portfolio view (FC-C5).
- PnL, exposure, and performance computation vs. benchmarks (FC-C6, FC-M1).
- Risk metrics: volatility, VaR, drawdown, correlation/covariance (FC-C4, "Risk management is a crucial part..." FC-Foundations).
- **Trace**: FC-C5, FC-C6, FC-M1.

## Phase 4 — Allocation, Optimization & Rebalancing

- Asset allocation engine supporting target mixes per risk profile (aggressive/moderate/conservative/income-oriented per FC-C4) (FC-C1, FC-C2, FC-P2).
- Optimization layer (e.g., mean-variance / MPT-inspired per "Passive management... may use modern portfolio theory" — used here in service of active decisions) (FC-A1).
- Diversification constraints and rebalancing triggers (threshold- or calendar-based) (FC-C2, FC-P3).
- **Trace**: FC-C1, FC-C2, FC-P2, FC-P3.

## Phase 5 — Active Alpha Layer (optional, gated)

- Only if it traces to active management (FC-A1): signal research, forecasting, decision support for buy/sell timing ("Active managers pay close attention to market trends..." FC-A1).
- Backtesting harness over the unified data layer.
- Explicit decision log in `thoughtProcess.md` before any alpha model is adopted.
- **Trace**: FC-A1, FC-M1.

## Phase 6 — Execution & Monitoring (pluggable)

- Order/intent model decoupled from any broker; broker adapters implement a uniform interface behind the intent model (FC-D7).
- Monitoring & review loop: drift reports, objective progress, rebalance proposals (FC-M1, FC-P4).
- **Trace**: FC-D7, FC-M1, FC-P4.

## Phase 7 — Final Documentation

- Rewrite `README.md` as the front-facing document: project overview, quick start, architecture summary combining `PLAN.md` + `TECHNICAL.md`.
- Final pass on all docs; tag release.

---

## Phase Status Board

| Phase | Name | Status | Gate |
|---|---|---|---|
| 0 | Documentation Foundation | ✅ Done | Docs exist; broker code removed |
| 1 | Core Domain Model | 🔜 Next | Entities + tests trace to FC-C3/C4/C6 |
| 2 | Multi-Vendor Data Layer | ⏳ | Unified schema + 2 vendors live |
| 3 | Portfolio Engine & Analytics | ⏳ | Consolidated view + risk metrics |
| 4 | Allocation & Rebalancing | ⏳ | Rebalance loop works on historical data |
| 5 | Active Alpha Layer | ⏳ optional | Traces to FC-A1 |
| 6 | Execution & Monitoring | ⏳ | Adapter interface + monitoring loop |
| 7 | Final Documentation | ⏳ | README combines PLAN + TECHNICAL |