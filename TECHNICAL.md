# TECHNICAL.md — Codebase Wiring

> **Anchor**: `FOUNDATIONAL_COMPONENTS.md` (FC-xx) defines *what* the system is; this document defines *how it is wired*. Any architectural change must update both.

## 1. Target Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    DATA VENDOR ADAPTERS                     │
│   VendorA │ VendorB │ Dukascopy │ flat files │ ... (FC-C1)  │
└──────────────┬──────────────────────────────────────────────┘
               │  raw, vendor-shaped data
               ▼
┌─────────────────────────────────────────────────────────────┐
│                 DATA NORMALIZATION LAYER                    │
│   unified schema · FX conversion · quality gates (FC-C2)    │
│   ← seeded by existing database/data/raw pipeline           │
└──────────────┬──────────────────────────────────────────────┘
               │  canonical market data (store)
               ▼
┌─────────────────────────────────────────────────────────────┐
│                     PORTFOLIO ENGINE                        │
│  domain model · aggregation · PnL · benchmarks (FC-C3..C6)  │
└──────┬───────────────────────┬──────────────────────────────┘
       │                       │
       ▼                       ▼
┌──────────────────┐   ┌──────────────────────────────────────┐
│  RISK & ANALYTICS│   │  ALLOCATION / OPTIMIZATION /         │
│  vol · VaR · dd  │   │  REBALANCING (FC-C1, C2, P2, P3)     │
│  (FC-M1, C4)     │   └──────────────┬───────────────────────┘
└──────────────────┘                  │
                                      ▼
                      ┌───────────────────────────────────┐
                      │  EXECUTION INTENT MODEL (FC-D7)   │
                      │  broker adapters implement the    │
                      │  uniform interface — pluggable    │
                      └───────────────────────────────────┘
```

## 2. Repository Layout (target)

```
portfolio_management/
├── FOUNDATIONAL_COMPONENTS.md   # authoritative domain model (anchor)
├── PLAN.md                      # project phases
├── TECHNICAL.md                 # this file — wiring
├── thoughtProcess.md            # planning prompts & decision evolution
├── README.md                    # reserved: final-phase front-facing docs
├── database/                    # existing vendor-agnostic data pipeline (Phase 2 seed)
│   ├── analytics/               # model research notebooks
│   └── data/raw/                # fetchers, store, scanner (Dukascopy etc.)
└── <new_package>/               # Phase 1 onward (name decided in Phase 1)
    ├── domain/                  # Portfolio, Position, Objective, RiskProfile (FC-C3, C4, C6)
    ├── data/                    # vendor adapters + normalization (FC-C1, C2)
    ├── engine/                  # aggregation, PnL, benchmarks (FC-C5, C6)
    ├── risk/                    # risk metrics (FC-C4, M1)
    ├── allocation/              # optimization, diversification, rebalancing (FC-C1, C2, P2, P3)
    └── execution/               # intent model + broker adapter interface (FC-D7)
```


### 3.2 Broker/Execution Adapter (orders out — FC-D7)

The engine produces **intents**, never broker orders. Brokers are optional adapters behind a uniform interface:

```python
class ExecutionVenue(Protocol):
    def submit(self, intent: OrderIntent) -> Ack: ...
    def positions(self) -> list[Position]: ...   # feeds back into the engine
    def account(self) -> AccountSnapshot: ...
```

The system runs fully without any `ExecutionVenue` (analysis/monitoring mode) — execution is a plug-in, not a dependency. This is the practical meaning of "broker-agnostic": no broker folder, package, or import exists in core code.

### 3.3 Domain Invariants (FC-C3, C5)

- A Portfolio aggregates Positions; total weight invariants and currency conversion are enforced in `domain/`, using normalized FX data from the data layer.
- Every computed metric (PnL, exposure, risk) is traceable to a benchmark context (FC-M1: performance is always evaluated *against* benchmarks and objectives).

## 4. Existing Code Map

| Existing asset | Disposition | Wires into |
|---|---|---|
| `database/data/raw/` fetchers (Dukascopy, batch update, store) | Keep — reference vendor adapters | `data/` (Phase 2, FC-C1/C2) |
| `database/data/pipeline.ipynb`, `analytics/model.ipynb` | Keep as research notebooks | Phase 4/5 prototyping |
| `mt5_portfolio/portfolio/` (optimizer, risk, backtest, ML — deleted) | Deleted per decision: fresh start; concepts re-implemented against the new domain model | `allocation/`, `risk/` (Phase 3/4) |
| Broker folders (CTraderOpenApiDemo, ibkr_portfolio, prop_firms — deleted) | Deleted: broker-specific code has no place in a broker-agnostic core | — |

## 5. Conventions

- Python 3.11+; pandas/numpy for data; SQLite or equivalent storage as already used in `database/`; no broker SDK ever imported from core modules — broker SDKs may only appear inside an `execution/` adapter.
- Every module's docstring cites the FC-xx component(s) it implements.
- Research lives in notebooks under `database/`; production code lives in the package. Promotions notebook → package require a `thoughtProcess.md` entry.

## 6. Removed / Forbidden

- No broker-specific folders or imports (`mt5_*`, `ibkr_*`, `ctrader_*`, prop-firm logic) anywhere in core code.
- No direct coupling of the portfolio engine to any single vendor's data format.
- The old `portfolio_management.sln` (referenced a non-existent C# project) is removed; this is now a Python-first repository.
## 3. Key Contracts

### 3.1 Vendor Adapter (data in — FC-C1, C2)

Every data vendor implements one interface; nothing downstream knows the vendor:

```python
class MarketDataProvider(Protocol):
    def instruments(self) -> list[Instrument]: ...
    def prices(self, symbol: str, start: date, end: date) -> pd.Series: ...
    def fx_rates(self, base: str, quote: str) -> pd.Series: ...
```

Normalization rules live in the data layer, not in adapters: one canonical price schema, one FX source of truth, quality gates (gaps, outliers, stale data) applied before storage.