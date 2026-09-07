# thoughtProcess.md — Planning Prompts & Thought Evolution

> A living log of the project's planning conversation and how decisions evolved. Newest entries at the bottom. Every major architectural decision should end up here with its rationale, so future-you can trace *why*.

---

## Entry 1 — Project origin (pre-session)

The repo began as a multi-broker portfolio tool: `CTraderOpenApiDemo` (C#), `ibkr_portfolio`, `mt5_portfolio` (Python), plus `prop_firms` research and a `database/` market-data pipeline. Early work focused on broker connectivity and ML alpha research (see git history: "alpha research: backtest loop", "ML model macro view alignment").

**Observation that triggered this re-architecture**: the valuable logic (optimizer, risk, backtesting, factor signals) was buried inside `mt5_portfolio/portfolio/` — reusable ideas trapped in a broker-shaped folder. Broker code was duplicated per account (acg, aquafunded, icmarkets), which doesn't scale.

## Entry 2 — "Broker agnostic + multi-vendor data"

> *User prompt*: "I want this portfolio management codebase become broker agnostic (delete all broker-related folders) and will use data from several vendors."

**Decision**: invert the architecture. Instead of N brokers each bringing their own pipeline, the core is broker-free; brokers (if ever needed) become pluggable adapters behind an execution-intent interface. Data flows in from *data vendors*, not brokers — a cleaner separation: vendors inform decisions (read-only), brokers execute (optional, pluggable).

## Entry 3 — Documentation-first approach

> *User prompt*: create FOUNDATIONAL_COMPONENTS.md (authoritative, anchored to Investopedia's portfolio-management article), PLAN.md (phases, no code), TECHNICAL.md (wiring), README.md reserved for the final phase, and thoughtProcess.md (this log).

**Decision**: the project is anchored to an external, well-established definition of portfolio management rather than to code we already wrote. This is deliberate: it lets us judge every future feature ("does this serve allocation / diversification / rebalancing / risk objectives?") against an independent standard. README is deferred to the end so it reflects reality, not aspiration.

## Entry 4 — Fresh start over salvage

> *User prompt*: "Delete the whole mt5_portfolio folder — start the new architecture from scratch guided by FOUNDATIONAL_COMPONENTS.md."

**Decision**: full purge, not salvage. Rationale: the existing modules (optimizer.py, risk.py, MLmodel.py, etc.) encode broker-era assumptions in their interfaces; porting them would smuggle those assumptions into the new design. The *ideas* survive (they informed TECHNICAL.md's Phase 3–5 scope), the *code* does not. `database/` survives as the one vendor-agnostic, still-relevant asset — it becomes the seed of the multi-vendor data layer.

## Entry 5 — Verbatim foundation

> *User prompt*: "I will copy and paste the Investopedia content to FOUNDATIONAL_COMPONENTS.md before I toggle to act mode."

**Decision**: the authoritative doc contains verbatim source content (avoids paraphrase drift), with project framing and a traceability layer added around it. Docs defer to FOUNDATIONAL_COMPONENTS, never the reverse.

---

## Entry 6 — Phase 1: Core Domain Model (pmcore)

> *User prompt*: "Let's proceed to phase 1."

**Decisions**:
- Package named **`pmcore`** (short, unambiguous core-library name); `domain/` implemented first, pure Python with zero dependencies — no pandas, no I/O, no broker/vendor imports (TECHNICAL.md §5/§6).
- **Money as Decimal value object** with same-currency-only arithmetic; FX conversion is always explicit through an `FxConverter` port (data layer implements it in Phase 2). Identity (base==quote, rate=1) allowed for convenience.
- **Position is long-only** for now (shorts raise); revisit later via FC traceability if ever needed.
- **Transactions carry fees** (FC-F1: cost awareness is a foundational component, so fees are first-class, not an afterthought).
- **Benchmark is a first-class domain object** (FC-M1): `Portfolio.drift_vs_benchmark` makes benchmark-relative evaluation a domain primitive, preparing Phase 3/4.
- **InvestorProfile/Objective/Constraints** encode the FC-P1 *assess* step as state that allocation (Phase 4) will consume.
- Multi-currency accounts settle only in their base currency for now; cross-currency settlement is deferred to the Phase 3 engine with explicit FX — noted in `Account.apply`.

**Validation**: 30 unit tests pass — money arithmetic/currency handling, weighted-average cost, mark-to-market, aggregation across accounts/currencies, weight-sum invariant (§3.3), benchmark drift, objective progress.

- Language & package layout final choice (Phase 1).
- Which two data vendors come first (Phase 2) — Dukascopy fetcher in `database/` is the reference candidate.
- Whether the active alpha layer (PLAN Phase 5) is pursued — must justify itself against FC-A1 (active management) before adoption.
- Execution: rebuild broker adapters later only if a live trading need emerges (FC-D7).

## Open Questions / Future Entries

- Language & package layout final choice (Phase 1). -> resolved: Python, `pmcore`
- Which two data vendors come first (Phase 2) - Dukascopy fetcher in `database/` is the reference candidate.
- Whether the active alpha layer (PLAN Phase 5) is pursued - must justify itself against FC-A1 before adoption.
- Execution: rebuild broker adapters later only if a live trading need emerges (FC-D7).
- Shorts, cross-currency settlement: deferred, revisit via FC traceability.
