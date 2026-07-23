# OWL Places — notes for AI agents

## Name and identity

The tool is called **OWL Places**. OWL stands for **O**penStreetMap **W**ikidata **L**ink.
The public URL is https://osm.wikidata.link/. Do not refer to it as "OSM Wikidata Matcher"
or "OSM ↔ Wikidata" — those are the old names.

## Design system

### Colour palette

| Name         | Hex       | Usage |
|--------------|-----------|-------|
| Navy         | `#1a2035` | Hero background, navbar, dark UI surfaces |
| Navy light   | `#232c47` | Navbar dropdown background |
| Amber        | `#c8973e` | Primary accent: OWL letters, buttons, active links, borders |
| Amber bright | `#dba94e` | Amber hover state |
| Cream        | `#e8dcc8` | Text on dark backgrounds |
| Cream muted  | `rgba(232,220,200,.55)` | Secondary text on dark backgrounds |
| Paper        | `#f5f3ee` | Light section backgrounds (steps, cards) |

The amber and navy pairing is inspired by old cartographic map ink on dark paper — consistent
with the mapping/geo nature of the tool.

### Typography

Both fonts are loaded from Google Fonts in `base.html` and therefore available on every page.

- **Lora** (serif, weights 600/700 + italic 400) — headings, titles, the place name in search
  results. Gives the tool a reference-book / atlas character.
- **DM Mono** (monospace, weights 400/500) — step numbers, meta pills, button labels, badges,
  elapsed timestamps in the matcher UI. Reinforces the technical/data nature of the tool.

Do not introduce other typefaces without good reason.

### Navbar

The Bootstrap `bg-primary` class is overridden globally in `matcher/static/css/style.css`.
All templates that use `class="navbar ... bg-primary"` automatically get the OWL palette —
do not add inline styles to navbars. The brand link in `navbar.html` wraps "OWL" in
`<span class="brand-owl">` so it renders in amber while "Places" stays cream.

### Key UI patterns

**Buttons** — three established styles used in search results and elsewhere:
- `.btn-run` — amber fill, navy text. Primary CTA (e.g. "Run matcher").
- `.btn-view` — navy fill, cream text. For viewing existing results.
- `.btn-browse` — amber outline. Secondary action (e.g. "Browse subdivisions").

**Status / left-border cards** — result cards use a 4px left border to communicate state
at a glance: amber = actionable, green (`#28a745`) = already matched, grey = unavailable.

**Meta pills** — small monospace tags (`.meta-pill`) for structured metadata like OSM type,
area, or category. Use `.pill-type` for the amber-tinted variant.

**Stage tracker** — the matcher progress sidebar (`matcher.html` + `ws.js`) uses a four-stage
pipeline UI: pending (grey circle), active (spinning blue border), done (green filled tick).

### Owl motif

The home page (`index.html`) includes a small inline SVG owl mark above the title. It is
amber-coloured with large round eyes and ear tufts. Keep it small (≤48px) and tasteful —
it is a brand mark, not a mascot. Do not add it to other pages.

## Template structure

- `base.html` — root layout; includes Google Fonts, Bootstrap 4, Fork Awesome, `style.css`.
- `navbar.html` — defines `navbar_inner()` and `navbar()` macros used by all pages.
- `index.html` — home page; extends `base.html`; overrides `{% block style %}` and
  `{% block content %}`.
- `results_page.html` — search results page; extends `base.html`; includes
  `search_results.html` for the card list.
- `matcher.html` — standalone full-screen layout (does not extend `base.html`); uses its
  own navbar and the map + sidebar layout.

## Frontend libraries (available, no CDN needed)

All served from `matcher/static/`:

- **Bootstrap 4** — `bootstrap4/`
- **Fork Awesome** — `fork-awesome/` (icon set; use `<i class="fa fa-*">`)
- **Leaflet** — `leaflet/` (maps)
- **jQuery** — `jquery/`

# Work Queue Replacement Options

The current system (`matcher_queue.py`, `matcher/job_queue.py`, `matcher/chat.py`) uses a
hand-rolled queue built on Python `threading`, `queue.PriorityQueue`, and a custom TCP socket
protocol on `localhost:6030`. This document evaluates replacements.

## Current Architecture

```
Browser  --WebSocket-->  Flask (websocket.py)
                              |
                         TCP socket :6030
                              |
                     matcher_queue.py (ThreadingTCPServer)
                         |              |
               Overpass worker     MatcherJob threads
               (process_queue)     (one per place)
                    |                    |
              PriorityQueue         JobManager
              (area-based)          (active_jobs dict)
```

**What gets queued:** Overpass API query chunks, prioritised by place area (smaller = higher
priority). Each `MatcherJob` thread orchestrates the full pipeline: Wikidata queries, Overpass
requests, osm2pgsql import, candidate matching.

**IPC:** Custom line-based JSON-over-TCP protocol (`\r\n` delimited). The `chat.py` module
provides `connect_to_queue()`, `send_command()`, `read_json_line()`.

**Pain points:** Custom protocol to maintain, no persistence (queue lost on restart), no retry
logic, no monitoring, single Overpass worker thread, tight coupling between queue server and
job execution.

## Existing Stack

| Component | Technology |
|-----------|-----------|
| Database | PostgreSQL + PostGIS |
| ORM | SQLAlchemy + GeoAlchemy2 |
| DB driver | psycopg2 |
| Web framework | Flask + Flask-Sock |
| WSGI server | gunicorn + gevent |
| External APIs | Overpass, Wikidata SPARQL, Nominatim |

No Redis, RabbitMQ, or other message broker is currently deployed.

---

## Recommendation: Procrastinate (PostgreSQL-only)

**Procrastinate** is the best fit. It is a PostgreSQL-native task queue that requires zero new
infrastructure and provides the features this project needs.

- **PyPI:** `procrastinate` (v3.7.2, January 2026)
- **GitHub:** ~1,200 stars, 59+ contributors, actively maintained
- **License:** MIT

### Why Procrastinate

1. **No new infrastructure.** Uses PostgreSQL tables for job storage and state. You already
   have PostgreSQL.

2. **Priority queues built in.** Priority is an integer field on each job. Higher numbers run
   first. Within the same priority, jobs run FIFO. This maps directly to the current
   area-based priority system (convert area to an integer priority: `priority = -int(area)`
   or bucket into bands).

3. **SQLAlchemy connector.** `procrastinate.contrib.sqlalchemy.SQLAlchemyPsycopg2Connector`
   reuses your existing SQLAlchemy engine and connection pool. From Flask, you defer tasks
   synchronously; a separate worker process picks them up.

4. **Retries, locks, periodic tasks.** Built-in retry strategies (linear, exponential),
   task-level locks (e.g., "only one match job per place at a time"), and cron-like periodic
   tasks.

5. **Job status in the database.** All job state lives in PostgreSQL tables, queryable via SQL
   or the Procrastinate API. Easy to build status endpoints.

6. **Worker process model.** Workers run as separate processes (`procrastinate worker`), not
   threads inside your web server. Cleaner separation of concerns.

### Migration sketch

```python
# In your Flask app setup:
from procrastinate.contrib.sqlalchemy import SQLAlchemyPsycopg2Connector

connector = SQLAlchemyPsycopg2Connector(engine=your_sqlalchemy_engine)
app_procrastinate = procrastinate.App(connector=connector, import_paths=["matcher.tasks"])

# matcher/tasks.py:
@app_procrastinate.task(queue="overpass", lock="overpass_api")
def run_overpass_chunk(chunk_filename, oql_query, chunk_num, place_id):
    """Execute one Overpass query chunk."""
    ...

@app_procrastinate.task(queue="matcher")
def match_place(osm_type, osm_id, user, remote_addr, user_agent, want_isa):
    """Full matching pipeline for a place."""
    ...

# Deferring from Flask:
run_overpass_chunk.configure(priority=-int(area)).defer(
    chunk_filename=fn, oql_query=oql, chunk_num=n, place_id=pid
)
```

Worker process:

```bash
procrastinate worker --app=matcher.tasks.app_procrastinate
```

### Caveats

- **Flask is not the primary integration target.** Django and async frameworks have more
  documentation. The SQLAlchemy connector works but you'll be following lower-level API docs.
- **psycopg2 may be dropped.** The project is moving toward psycopg3. Plan to migrate from
  psycopg2 to psycopg3 eventually.
- **Real-time browser updates need rethinking.** The current system streams status via
  WebSocket <-> TCP socket. With Procrastinate, job status lives in the database. Options:
  - Poll the job status table from the browser (simplest).
  - Use PostgreSQL `LISTEN/NOTIFY` to push status changes to a WebSocket endpoint.
  - Have the task write status to a Redis pub/sub or SSE stream (adds infrastructure).

---

## Runner-up: PGQueuer

**PGQueuer** is a lighter-weight PostgreSQL queue using `SKIP LOCKED` + `LISTEN/NOTIFY`.

- **PyPI:** `pgqueuer` (v0.25.3, December 2025)
- **GitHub:** ~1,400 stars, actively maintained
- **License:** MIT

### Pros over Procrastinate
- Higher throughput (~6,400 jobs/sec with asyncpg).
- Uses `LISTEN/NOTIFY` for instant worker wakeup (Procrastinate uses polling).
- Built-in terminal dashboard for monitoring.
- Explicit `SyncPsycopgDriver` for Flask/WSGI enqueueing.

### Cons vs Procrastinate
- **Requires psycopg3** (not psycopg2). Would need a driver migration first.
- **Requires Python 3.11+.**
- Smaller community (13 contributors).
- Fewer built-in features (less retry/periodic task support).
- Workers must be async; only enqueueing has a sync driver.

### When to pick PGQueuer instead
If you're already planning to migrate to psycopg3 and Python 3.11+, and you value raw
throughput and `LISTEN/NOTIFY` responsiveness over built-in retry/periodic task features.

---

## If Willing to Add Redis: Celery

**Celery** is the industry standard Python task queue. Adding Redis gives you the most mature
ecosystem.

- **PyPI:** `celery` (v5.6.x), `redis` (broker)
- **GitHub:** ~28,000 stars
- **License:** BSD

### Pros
- Largest ecosystem, best documentation, most StackOverflow answers.
- Excellent Flask integration (well-documented pattern).
- Flower monitoring dashboard, task chaining, canvas workflows.
- Battle-tested at massive scale.

### Cons
- **Requires Redis** (or RabbitMQ). Adds operational complexity.
- Celery with PostgreSQL as broker is broken: no message cleanup, no events, polling-only.
  The maintainers discourage this configuration.
- Complex configuration surface area.
- Heavier dependency footprint.

### When to pick Celery
If you anticipate needing complex workflows (task chains, groups, chords), or if Redis is
already planned for caching or other purposes.

---

## Options to Avoid

| Option | Why |
|--------|-----|
| **Celery + PostgreSQL broker** | Fundamentally broken: no message cleanup, no events, no LISTEN/NOTIFY. Maintainers discourage it. |
| **Dramatiq** | The PostgreSQL broker extension (`dramatiq-pg`) is inactive/unmaintained. |
| **RQ** | Requires Redis with no alternative. |
| **PGMQ** | Message queue, not a task queue. Requires installing a PostgreSQL extension. You'd build all task semantics (retries, workers, scheduling) yourself. |
| **Huey + PostgreSQL** | The PG backend uses peewee ORM, adding a second ORM alongside SQLAlchemy. |
| **Raw SKIP LOCKED** | Sound pattern, but building production-grade retry, monitoring, worker lifecycle, and crash recovery from scratch is a large undertaking that Procrastinate/PGQueuer already solve. |

---

## Summary

| | Procrastinate | PGQueuer | Celery + Redis |
|---|---|---|---|
| New infrastructure | None | None | Redis |
| Priority queues | Built-in (integer) | Supported | Via broker |
| Flask integration | SQLAlchemy connector | Sync driver | Excellent |
| Retries | Built-in | Basic | Built-in |
| Periodic tasks | Built-in | Cron-like | Celery Beat |
| Real-time status | SQL query / NOTIFY | LISTEN/NOTIFY + dashboard | Flower + events |
| Maturity | Good (1.2k stars) | Growing (1.4k stars) | Excellent (28k stars) |
| DB driver | psycopg2 (for now) | psycopg3 only | N/A |

**Start with Procrastinate.** It requires no new infrastructure, integrates with your existing
SQLAlchemy/PostgreSQL setup, and provides priority queues, retries, and locks out of the box.
