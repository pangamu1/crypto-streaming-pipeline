# ADR-0002: Airflow runtime — plain Docker image in project compose (supersedes the `astro dev` choice)

- Status: Accepted
- Date: 2026-06-30
- Deciders: Piruthviraj
- Supersedes: the CP0 decision "Airflow runtime = Astronomer `astro dev`"

## Context and Problem Statement
CP0 locked Airflow's local runtime as Astronomer's `astro dev` CLI. On
resuming CP4, the `astro` CLI was not installed, and installing it (via
Homebrew) adds a global binary to the machine. We want to avoid extra
system-level installs and keep the whole stack self-contained in Docker.

## Considered Options
- Astronomer `astro dev` CLI (the original CP0 choice) — wraps Airflow in
  Docker, but requires installing the `astro` binary system-wide.
- Official Apache Airflow `docker-compose.yaml` — canonical, but ~5–6
  containers (webserver, scheduler, triggerer, metadata Postgres, redis).
- A single `apache/airflow` "standalone" service added to this project's
  existing `docker-compose.yml`.

## Decision Outcome
Chosen: **a single `apache/airflow` standalone service in the project's
existing compose file.** Rationale:
- Zero new system installs — Airflow runs entirely in Docker, like the rest
  of the stack. `astro dev` is itself only a Docker wrapper, so little is lost.
- Lightest footprint: one extra container vs the official compose's five-plus.
- It joins the same Docker network as Postgres, so the DAG reaches the trades
  sink by service name (`postgres:5432`) — no host-port juggling, no
  `host.docker.internal`.
- Fastest path to a green scheduled run for a bounded, local demo.

## Consequences
- Good: self-contained `docker compose up`; nothing global to install.
- Good: DAG → sink connection is internal and stable.
- Accepted cost: standalone uses SQLite + SequentialExecutor (no task
  parallelism). Fine for a bounded local demo; not a production topology —
  documented honestly, matching this project's "local demo, not always-on
  cloud" framing.
- Host port: Airflow UI is published on **8081** (8080 is taken by kafka-ui).
- Reversible: can be superseded if a heavier/parallel runtime is ever needed.
