# A local run with lineage on and Marquez down is slow, not failed

Measured 2026-08-15 against `openlineage-python` as pinned by `celine-utils[pipelines]`.

`PipelineConfig.openlineage_enabled` defaults to **true**, so `PipelineRunner` builds an
`OpenLineageClient` whether or not anything is listening on `OPENLINEAGE_URL`
(`http://host.docker.internal:5003` by default — the `marquez-api` compose service, which
is not started by `docker compose up datasets-db`).

## What actually happens

The OpenLineage HTTP transport ships a urllib3 retry policy of `total=5, connect=5,
read=5, backoff_factor=0.3` with a 5-second connect timeout. A single `emit()` against a
**refused** port therefore takes **~9 seconds** before raising:

```text
elapsed refused-port: 9.01s
```

An unreachable host that blackholes rather than refuses is worse — each of the six
attempts pays the full connect timeout.

`PipelineRunner._emit_event` wraps `client.emit()` in `try/except Exception` and calls
`logger.exception`. So the failure never propagates: **the run succeeds**, and the only
symptoms are the wall-clock and a traceback per event in the log.

`_emit_event` is called at both ends of every stage — there are nine call sites in
`pipeline_runner.py`. A four-stage flow emits roughly eight events, which is over a minute
of pure retry wait on a pipeline whose SQL takes seconds.

## Why this is easy to misread

The slow part is not attributable to anything in the log that looks like waiting. dbt
reports its own model timings, and they are all fast; the gap sits *between* stages, where
nothing prints. The natural next move — profiling the query, checking the database, adding
indexes — investigates the one part of the system that is behaving.

## What to do

Decide at the start of a session, not after wondering:

```bash
docker compose up marquez-api marquez-web -d     # keep lineage, UI on :5002
export OPENLINEAGE_ENABLED=false                 # or skip it entirely
```

`OPENLINEAGE_ENABLED=false` skips client construction outright and logs
`OpenLineage disabled (OPENLINEAGE_ENABLED=false)` once. It is the right default in `.env`
for anyone not working on lineage.

Note that `marquez-api` depends on `datasets-db` for its own database — starting Marquez
without the database gives you a container that is up and still refusing connections,
which looks like the good case and behaves like the bad one.

## What this does *not* cover

MQTT pipeline events (`MQTT_EVENTS_ENABLED`, also default true) are emitted from a daemon
thread and are genuinely fire-and-forget; the completion hook joins for at most 12 seconds.
They are not this problem.
