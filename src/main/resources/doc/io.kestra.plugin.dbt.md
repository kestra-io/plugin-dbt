# How to use the dbt plugin

The `DbtCLI` task runs any dbt command inside a configurable container and parses run results automatically.

## Authentication

Authentication to your data warehouse lives in the `profiles` property, which accepts the full content of a `profiles.yml` file as an inline string. Use [`{{ secret('NAME') }}`](https://kestra.io/docs/concepts/secret) to inject credentials at runtime rather than hardcoding them. For a cleaner separation of configuration from flow logic, store `profiles.yml` as a [namespace file](https://kestra.io/docs/concepts/namespace-files) and reference it with namespace file functions.

## Common properties

All CLI tasks run in a container defined by `containerImage`. Use an image from [ghcr.io/kestra-io/docker-dbt](https://github.com/kestra-io/docker-dbt) that matches your adapter — `dbt-postgres`, `dbt-bigquery`, `dbt-snowflake`, `dbt-duckdb`, and others are available. The `taskRunner` property defaults to Docker and can be overridden for other execution environments.

## Tasks

`DbtCLI` is the primary task and runs any dbt CLI command. For state-based selection across runs, `storeManifest` and `loadManifest` persist `manifest.json` to and from the Kestra KV Store. Dedicated per-command tasks (`Run`, `Test`, `Seed`, `Build`, and others) also exist but are **deprecated** — use `DbtCLI` with the corresponding dbt command (for example `commands: ["dbt build"]`) instead.

For dbt Cloud, use `TriggerRun` — it starts a job and waits for completion by default without requiring a container. Use `CheckStatus` to poll a run that was triggered outside of Kestra. Authenticate dbt Cloud tasks with `accountId` (your numeric dbt Cloud account ID) and `token` (a dbt Cloud API token, supplied via `{{ secret('NAME') }}`).

When `wait` is true, `TriggerRun` remembers the dbt Cloud run ID in the flow's namespace KV Store (a short-lived entry, TTL of twice `maxDuration`). If the worker restarts while the run is in flight, `TriggerRun` resumes that run instead of triggering a duplicate. Set `resumeOnRestart: false` to opt out of this behavior.
