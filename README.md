# 🧠 Portfolio Management System

A **broker-agnostic, active portfolio management system**. Market data flows in from **multiple data vendors** (normalized into one internal schema); brokers are strictly optional pluggable adapters. The system consolidates positions across accounts and provides **analytics, risk monitoring, optimization, and rebalancing** for a self-directed, active, discretionary portfolio manager.

> **📚 Documentation map**
> - **[`FOUNDATIONAL_COMPONENTS.md`](FOUNDATIONAL_COMPONENTS.md)** — the authoritative domain model (Investopedia-anchored, with traceability IDs FC-xx). Every feature must trace here; anything else is out of scope.
> - **[`PLAN.md`](PLAN.md)** — project phases (0–7), each gated against the foundational components.
> - **[`TECHNICAL.md`](TECHNICAL.md)** — codebase wiring: architecture, adapter contracts, module layout.
> - **[`thoughtProcess.md`](thoughtProcess.md)** — planning prompts and decision evolution.

---

## 🎯 What It Does

The system implements the classic portfolio-management loop: **assess → allocate → diversify → rebalance → monitor**.

- **Multi-vendor market data** — pluggable `MarketDataProvider` adapters (Dukascopy, vendor APIs, flat files); one canonical price/FX schema with quality gates. No vendor is privileged.
- **Consolidated portfolio view** — aggregate positions across accounts into one portfolio; PnL, exposure, and benchmark-relative performance.
- **Risk & analytics** — volatility, VaR, drawdown, covariance, risk-adjusted return vs. objectives.
- **Allocation & rebalancing** — target mixes per risk profile (aggressive / moderate / conservative / income-oriented), threshold- or calendar-based rebalancing triggers.
- **Pluggable execution** — the engine emits *intents*, never broker orders. Broker adapters implement a uniform `ExecutionVenue` interface and can remain entirely unimplemented: the system runs fully in analysis/monitoring mode.

**Out of scope by design**: passive index-tracking (used only for benchmark definition), broker SDKs in core code, retail compliance machinery.

---

## 🧩 Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                    DATA VENDOR ADAPTERS                     │
│   VendorA │ VendorB │ Dukascopy │ flat files │ ... (FC-C1)  │
└──────────────┬──────────────────────────────────────────────┘
               ▼
┌─────────────────────────────────────────────────────────────┐
│                 DATA NORMALIZATION LAYER                    │
│   unified schema · FX conversion · quality gates (FC-C2)    │
└──────────────┬──────────────────────────────────────────────┘
               ▼
┌─────────────────────────────────────────────────────────────┐
│                     PORTFOLIO ENGINE                        │
│  domain model · aggregation · PnL · benchmarks (FC-C3..C6)  │
└──────┬───────────────────────┬──────────────────────────────┘
       ▼                       ▼
┌──────────────────┐   ┌──────────────────────────────────────┐
│  RISK & ANALYTICS│   │  ALLOCATION / OPTIMIZATION /         │
│  vol · VaR · dd  │   │  REBALANCING (FC-C1, C2, P2, P3)     │
└──────────────────┘   └──────────────┬───────────────────────┘
                                      ▼
                      ┌───────────────────────────────────┐
                      │  EXECUTION INTENT MODEL (FC-D7)   │
                      │  broker adapters — pluggable      │
                      └───────────────────────────────────┘
```

Full details, contracts, and repository layout in **[`TECHNICAL.md`](TECHNICAL.md)**.


---

## 🗺️ Roadmap

| Phase | Name | Status |
|---|---|---|
| 0 | Documentation Foundation | ✅ Done |
| 1 | Core Domain Model | 🔜 Next |
| 2 | Multi-Vendor Data Layer | ⏳ |
| 3 | Portfolio Engine & Analytics | ⏳ |
| 4 | Allocation & Rebalancing | ⏳ |
| 5 | Active Alpha Layer (optional) | ⏳ |
| 6 | Execution & Monitoring | ⏳ |
| 7 | Final Documentation | ⏳ |

Details and gate criteria per phase in **[`PLAN.md`](PLAN.md)**.

---

## 🧱 Repository Structure

```
portfolio_management/
├── FOUNDATIONAL_COMPONENTS.md   # authoritative domain model (anchor)
├── PLAN.md                      # project phases
├── TECHNICAL.md                 # architecture & wiring
├── thoughtProcess.md            # planning prompts & decision log
├── README.md                    # this file
└── database/                    # vendor-agnostic market-data pipeline (Phase 2 seed)
    ├── analytics/               # model research notebooks
    └── data/raw/                # fetchers (Dukascopy etc.), store, scanner
```

The production package (`domain/`, `data/`, `engine/`, `risk/`, `allocation/`, `execution/`) is introduced from Phase 1 onward — see the target layout in **[`TECHNICAL.md`](TECHNICAL.md)**.

---

## 🔒 Disclaimer

This project is for **educational and research purposes only**.
It **does not constitute financial advice**, and is not a solicitation to invest or trade.
Use at your own risk and comply with any data vendor's terms and local regulations.

---

## 🧬 License

MIT License © 2025

---

## 🤝 Contributing

Pull requests are welcome!
For major changes, please open an issue first — and note that all changes must trace to a component in **[`FOUNDATIONAL_COMPONENTS.md`](FOUNDATIONAL_COMPONENTS.md)**.

