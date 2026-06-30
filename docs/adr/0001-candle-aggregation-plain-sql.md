# ADR-0001: Candle aggregation via plain SQL (not dbt)

- Status: Accepted
- Date: 2026-06-30
- Deciders: Piruthviraj

## Context and Problem Statement
CP4's Airflow DAG must turn raw `trades` into 1-minute and 1-hour OHLC
candles. How should the transformation be expressed — plain SQL run by
Airflow, or dbt-postgres models triggered by Airflow?

## Considered Options
- Plain SQL via Airflow's Postgres operator
- dbt-postgres models triggered by Airflow

## Decision Outcome
Chosen: **plain SQL**. CP4's purpose is to demonstrate Airflow
orchestration plus a data-quality gate, and plain SQL is the leanest path
to a green scheduled run. dbt is already showcased in the sibling FinTech
project, so using it here would be redundant and add setup (dbt project,
profiles, dbt–Airflow wiring) without strengthening this project's
streaming + orchestration story.

### Consequences
- Good: fewest moving parts; fastest to green; orchestration stays the focus.
- Good: no dbt overlap with FinTech — each project demonstrates a distinct skill.
- Accepted cost: transformation logic lives in SQL files, not a dbt model
  graph (no dbt lineage/tests here). Data quality is enforced by an explicit
  Airflow gate task instead.
- Reversible: can be superseded by a later ADR if a dbt layer is added.
