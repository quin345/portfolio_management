# FOUNDATIONAL COMPONENTS

> **AUTHORITY**: This document is the single source of truth for the portfolio management system. Every feature, module, and phase in `PLAN.md` and `TECHNICAL.md` MUST trace to a component defined here. If a proposed change cannot be traced to this document, it is out of scope.
>
> **Context**: This project is a broker-agnostic, multi-vendor-data portfolio management system for *active discretionary* portfolio management (see [Project Traceability](#project-traceability) at the bottom). It consolidates positions across accounts, ingests market data from several data vendors, and provides analytics, risk monitoring, optimization, and rebalancing.
>
> **Source**: Framework and definitions based on Investopedia — [Portfolio Management: Definition, Types, and Strategies](https://www.investopedia.com/terms/p/portfoliomanagement.asp) (content pasted verbatim below; project alignment notes added around it).

The framework is decomposed into traceable component IDs. Every module in `TECHNICAL.md` and phase in `PLAN.md` cites these.

## Core Components (FC-C)

| ID | Component (from source content above) |
|---|---|
| FC-C1 | **Asset allocation** — the right mix of asset classes optimized for accepted risk |
| FC-C2 | **Diversification** — spreading holdings within/across asset classes to reduce single-investment risk |
| FC-C3 | **Investment objectives** — long-term goals; maximize expected return within appropriate risk |
| FC-C4 | **Risk tolerance & risk profiles** — aggressive / moderate / conservative / income-oriented / tax-efficiency; risk vs. return trade-off |
| FC-C5 | **Portfolio oversight** — building and overseeing a selection of assets; weighing strengths/weaknesses, opportunities, threats, trade-offs (debt vs. equity, domestic vs. international, growth vs. safety) |
| FC-C6 | **Performance evaluation** — total return, risk-adjusted return, consistency, progress vs. objectives |
| FC-D7 | **Execution (broker) layer** — where investment decisions are carried out; strictly pluggable/optional in this system |
| FC-F1 | **Fees & tax efficiency** — cost awareness in every decision |

## Management Approaches (FC-A / FC-D)

| ID | Approach |
|---|---|
| FC-A1 | **Active management** — strategic buying/selling to beat a benchmark; uses quantitative/qualitative models, market/economic/news research |
| FC-A2 | **Passive management** — index matching; referenced for benchmark definition, not this system's mode |
| FC-D7 | **(see Core)** — discretionary execution decision-making delegated to the manager; this project is discretionary-active by default |

## Process Loop (FC-P)

| ID | Step |
|---|---|
| FC-P1 | **Assess** investor situation: objectives, risk tolerance, investment horizon, liquidity/flexibility constraints |
| FC-P2 | **Allocate** across asset classes per the chosen risk profile |
| FC-P3 | **Diversify & rebalance** — maintain the desired mix as markets move |
| FC-P4 | **Monitor & review** — ongoing oversight; reassess when life circumstances, tax rules, or goals change |

## Monitoring (FC-M)

| ID | Component |
|---|---|
| FC-M1 | **Benchmark comparison** — performance is always evaluated against benchmarks and objectives (total return, risk-adjusted return, consistency) |

## Scope Decisions for This Project

- **In scope**: FC-C1..C6, FC-A1 (active, discretionary), FC-P1..P4, FC-M1, FC-F1. Multi-account consolidation; multi-vendor market data; analytics, risk, allocation, rebalancing, monitoring.
- **Out of scope / deferred**: retail compliance/fiduciary regulatory machinery (the Retirement Security Rule section is context/informational only); passive index-tracking engine (FC-A2 is used only to define benchmarks); execution (FC-D7) is pluggable and may remain unimplemented.
- **Users served**: individual investor managing own portfolios (per "Users of Portfolio Management" section) — self-directed, active, discretionary.
