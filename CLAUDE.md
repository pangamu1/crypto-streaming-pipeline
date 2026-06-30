# CLAUDE.md — Crypto Streaming Pipeline Project Context

> Sibling project to **fintech-medallion-portfolio**. That project is deliberately **batch-only**; this one is the dedicated home for the two skills kept out of it: **streaming (Kafka)** and **batch orchestration (Airflow)**. Operating rules are inherited from fintech; architecture/conventions are this project's own.

## Behavioral Rules
- Never assume or speculate about files you have not opened. Always read before answering.
- Always provide hallucination-free answers grounded in what is actually present in the code.
- Never state a specific value, config, or behavior as fact without having read the file that contains it. If uncertain, say "let me verify" and read the file before answering.
- When answering questions about configuration or behavior, always read the relevant file first — do not rely on memory or inference from prior context.
- When referencing code, include the file path and line number where relevant.
- Do not create files unless explicitly requested.
- Be direct and concise. Lead with the answer, not the reasoning.
- Be an extremely harsh critic when the user clarifies doubts. Do not validate with phrases like "Exactly right", "You've got it", "You nailed it", "Spot on", or any equivalent. Only confirm correctness when the answer is precisely correct per official documentation. If the user is even slightly off, correct them immediately and explain why.
- **NEVER fabricate API behavior, response shapes, rate limits, auth models, or tier policies.** If docs aren't readable, have the user paste the relevant section. This project's central unknown is the Coinbase WS auth model — verify by doing, never assume.

## Project Goal
End-to-end **real-time + batch** data engineering portfolio project. Demonstrates streaming (Kafka) and batch orchestration (Airflow) **honestly** — both tools are justified by a genuine real-time source (live crypto trades) and a genuine batch need (periodic rollups + DQ), not decorative. Target: MAANG-level production quality. Free + local only; no paid services.

## Architecture (Authoritative)
```
Coinbase Advanced Trade WS  →  Producer (asyncio)  →  Kafka topic  →  Stream processor  →  Sink (DuckDB/Postgres/Parquet)
                                                                                                      ↑
                                                              Airflow (scheduled): raw trades → OHLC candles + DQ gate
```

**Core design discipline — non-negotiable (this is what makes or breaks credibility):**
- **Kafka = data in motion.** Real-time trade events buffered in a topic.
- **Airflow = data at rest, on a schedule.** Periodic rollups / DQ over what the stream already landed. **Airflow NEVER touches the live socket** — scheduling a long-lived stream as an Airflow task is the anti-pattern to avoid.
- **Stream processing ≠ Airflow.** Real-time transforms are done by a stream processor (Spark Structured Streaming / Kafka Streams / Faust / plain consumer), not Airflow.
- **Honest framing:** streaming is continuous but there's no free 24/7 host — the demo runs **bounded-window on-demand** (producer captures ~10–30 min) + **scheduled batch**. Document this; never pretend it's an always-on cloud deployment.

## Reference Documents
- **Plan / lesson roadmap:** `~/.claude/plans/project-crypto-stream-kafka-airflow.md` — CP0–CP6 checkpoints + open questions. Resume from here. (Lives outside the repo, shared across sessions.)
- ADRs under `docs/adr/` — append-only decision log in [MADR format](https://adr.github.io/madr/). Write a new ADR for every significant choice; reverse by superseding, never by editing history. (To be created as decisions are locked.)
- Auto-memory at `~/.claude/projects/<project-id>/memory/` — operational connection facts, role, preferences (not in repo).

## Working Style — LEARNING MODE (inherited from fintech, locked)
- **Claude does NOT write code or create files directly.** Project source files (`*.py`, `*.toml`, `*.yml`, `docker-compose.yml`, `.gitignore`, `.env*`, DAGs, etc.) are typed entirely by the user. Non-negotiable.
- Claude's job: explain the concept, explain *why* a line/block exists, then guide the user to type it themselves.
- Work in **small phases / blocks / snippets** — never an entire file in one go. Checkpoints between chunks.
- The user types every line. The user runs every command. Claude observes output and explains what it means.
- Even if the user says "write this for me", redirect: ask which part they want to learn first, then walk them through writing it.
- **Exception — meta files:** the plan file at `~/.claude/plans/` and this `CLAUDE.md` are NOT project source code; Claude edits them directly. The user reviews the diff.
- **Why:** the point is portfolio-grade *understanding*. Code the user can't explain in an interview is worse than no code at all.

## Response Style for LEARNING MODE Teaching
Each teaching turn for a checkpoint/sub-step MUST follow this shape:
1. **What and why** (1–3 sentences) — name the concern, lead with the answer.
2. **Reference shape** — the EXACT code to type, in a fenced block. Annotate every meaningful decision (style choices, library quirks, deliberate omissions) in a follow-up table.
3. **Verification commands** — logic-only checks (`uv run python -c "..."`, AST, `grep`, file inspection) BEFORE any live run.
4. **Expected output** — show what success looks like literally.
5. **Pointer to next sub-step** — "paste back" + what closes after verification.

Size limits: algorithmic code ≤ ~30 lines per reference shape (split if larger); config/data declarations up to ~80 lines is fine; tooling-generated files (`pyproject.toml`, `uv.lock`) are shown via `cat`, not typed.

The user adapts comments/naming/docstrings to their own voice — does NOT paste reference shapes verbatim.

**Verification-first cadence:** logic-verify before any live run; smoke-test with the minimum scope (1 product / 60s before a full window).

## Conventions (inherited)
- **User creates branches and the repo.** Claude drafts in a worktree if needed; the user copies over. Don't create branches in the user's main checkout.
- **PRs opened in the GitHub web UI.** Stop at `git push`; never run `gh pr create`.
- **Branch naming:** `init/`, `feat/`, `chore/`, `fix/` prefixes. Merge strategy: squash-and-merge, delete branch after merge.
- **Python toolchain:** `uv` (Astral) for venv + deps; lockfile committed; **application mode**, not library.
- **Secrets:** never committed; via env vars / `.env` (gitignored); `.env.example` uses angle-bracket placeholders (`<your-key>`).
- **Logging:** library modules use `logger = logging.getLogger(__name__)`; entry-point scripts call `logging.basicConfig(...)` once.
- **The user monitors runs in the UI** (Airflow UI, Kafka/Redpanda console, GHA). Don't poll status via API/curl; the user pastes screenshots.
- **GUI work:** give exact menu paths, one step at a time; verify by reading actual values, not eyeballing.

## Free / Local Stack (provisional — locked at CP0)
- **Source:** Coinbase Advanced Trade WebSocket (market data free; auth model TBD at CP0).
- **Broker:** ✅ **Apache Kafka (KRaft mode)** — local Docker (real Kafka, no ZooKeeper).
- **Stream processor:** ✅ **Spark Structured Streaming** — `readStream` Kafka → `foreachBatch` JDBC → Postgres, checkpointing.
- **Sink:** ✅ **Postgres** (Docker) — concurrent stream-write/batch-read; dbt-postgres symmetry with fintech.
- **Airflow runtime:** ✅ **Astronomer `astro dev`**.
- **Cost: ZERO** — all infra local Docker, market data free.

## Current Progress
- **2026-06-22** — Repo created (`crypto-streaming-pipeline`, public, MIT + Python `.gitignore`), cloned locally, opened in VS Code via the Claude Code extension. Starter `CLAUDE.md` ported from fintech.
- **2026-06-22 — CP0 ✅ RESOLVED.** Coinbase WS auth verified by live spike: `market_trades` is **PUBLIC, no JWT** (producer skips JWT signing). Parse contract locked: `events[*].trades[*]` → `product_id, trade_id, price(str), size(str), time, side`; price/size are strings (→ Decimal/NUMERIC); dedup on **`(product_id, trade_id)`** (corrected at CP3 — `trade_id` is a per-product sequence, so the composite is the only always-correct duplicate identity); monotonic `sequence_num`. Decisions locked: **Kafka (KRaft) broker · Spark Structured Streaming processor · Postgres sink · `astro dev` Airflow · BTC-USD+ETH-USD, 1-min+1-hour candles · Learning mode.** Rationale + full contract in the plan file's CP0 section.
- **2026-06-22 — CP1 ✅ DONE.** `docker-compose.yml` at repo root: `kafka` (apache/kafka:3.8.0, KRaft single-node, dual listeners HOST `localhost:9092` / INTERNAL `kafka:29092` / CONTROLLER 9093), `postgres` (postgres:16, db/user/pw all `crypto`, named volume `pgdata`, healthcheck), `kafka-ui` (kafbat/kafka-ui at `localhost:8080`). Topic `trades.raw` (3 partitions, RF 1). Produce/consume smoke green.
- **2026-06-22 — CP2 ✅ DONE.** `uv` app project (managed CPython 3.12.13), deps `websockets==16.0` + `confluent-kafka==2.14.2`. `producer/main.py`: asyncio Coinbase WS → `trades.raw`, keyed by `product_id`; reconnect with exponential backoff (1→30s cap); graceful shutdown via `asyncio.Event` (SIGINT/SIGTERM handlers, no traceback) + `--duration` bounded-window timer; `producer.flush()` on exit. Verified: keyed JSON trades land in topic; 30s bounded run stops cleanly. **Committed + squash-merged to `main` via PR `feat/infra-and-producer` (2026-06-22).**
- **2026-06-27 — CP3 ✅ DONE.** `processor/main.py` (Spark Structured Streaming, pyspark 3.5.8 in uv venv, Java 17): `readStream` Kafka `trades.raw` (`localhost:9092`) → `from_json` vs locked 6-field schema → cast price/size→`DecimalType(38,18)`, time→timestamp (session tz **UTC**) → `dropDuplicatesWithinWatermark(["product_id","trade_id"])` (watermark 2h) → `foreachBatch` psycopg2 `execute_values` `INSERT … ON CONFLICT (product_id, trade_id) DO NOTHING` → Postgres `trades` + durable checkpoint `./.checkpoints/trades`. Kafka connector via `spark.jars.packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8`; **no JDBC driver** (psycopg path). Sink table `sql/trades.sql`: composite PK `(product_id, trade_id)`, `NUMERIC(38,18)`, `TIMESTAMPTZ`, `ingested_at DEFAULT now()`. Deps added: `pyspark>=3.5,<3.6`, `psycopg2-binary`. **Decision corrected:** dedup key is composite `(product_id, trade_id)`, not bare `trade_id` (per-product sequences). **Gotchas fixed:** (1) psycopg2 keyword DSN can't take `options=-c timezone=UTC` as a flat string (space = param delimiter) → pass kwargs dict; (2) host `5432` clashed with a native macOS Postgres bound to `::1` → remapped container to host **5433** (internal stays 5432). Idempotency proven (full re-read after checkpoint wipe held total at 2015); timestamps land UTC `+00` matching source `Z`. **Committed + squash-merged to `main` via PR #2 (`feat/spark-processor`, commit `dc26b67`, 2026-06-27).** Next: CP4 — Airflow batch DAG (1-min/1-hour OHLC candles + DQ gate; reads ONLY the sink).
