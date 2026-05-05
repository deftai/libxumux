# Cost & Budget Estimate

> All figures in **US dollars (USD)**. Loose ranges, not exact numbers.
> Built from the approved project spec. If the spec changes, redo this
> estimate before building.

## TL;DR

libxumux is a client library — it has no servers, no databases, and no
third-party API calls. Recurring monthly cost: **$0**. The only real
cost is the time to build and occasionally maintain it.

## What you will need to sign up for

- npm account (free) — to publish the library

That's it. No cloud accounts, no API keys, no domain names needed.

## Hosting & infrastructure

None. libxumux is a library that runs inside the caller's environment.
There is nothing to host.

## API & third-party fees

None. The library implements the xumux protocol itself — it makes no
outbound API calls and has no runtime service dependencies.

## Monthly band

- **Low** _(nothing published yet / local dev only)_: $0 / month
- **Typical** _(published on npm, in use by consumers)_: $0 / month
- **High** _(any scenario)_: $0 / month

Libraries have no recurring running costs. The monthly band is $0
regardless of how many people use it.

## Scale considerations

None. npm serves downloads for free at any scale. If consumers of this
library hit scale problems, those costs appear in *their* projects, not
this one.

## Build & maintenance time

A rough sense of effort — not a quote.

- **Build**: about 15 - 30 hours of focused work across 5 scopes
  (project scaffold, binary codec, control channel, transport adapters,
  connection API). The protocol is well-specified, which keeps scope
  tight; transport adapters (especially QUIC) are the main wildcard.
- **Maintenance**: about 2 - 5 hours per month after the first release
  (keeping up with xumux spec updates, TypeScript version bumps,
  security patches in optional peer dependencies).

## Decision point

Pick **one**. The build phase will refuse to start until this is recorded.

1. **Build** — proceed to build with this cost expectation.
2. **Rescope** — keep building but reduce cost first. List the spec
   changes, then redo this estimate.
3. **No-build** — stop here. Record the reason below.
4. **Skip** — skip the cost phase. Record a short reason
   (e.g. "hobby project, cost is not a concern").

### Decision recorded

- **Decision**: build
- **Date**: 2026-05-04
- **Recorded by**: Viz
- **Reason**: n/a (build decision)

---

_This estimate is a snapshot. Vendor pricing changes over time. Redo
this file before any major scope change. Methodology lives in
[directive/references/cost-models.md](./directive/references/cost-models.md)._
