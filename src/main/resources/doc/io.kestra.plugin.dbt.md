# How to use the dbt plugin

The `DbtCLI` task runs any dbt command inside a configurable container and parses run results automatically.

## Authentication

Authentication to your data warehouse lives in the `profiles` property, which accepts the full content of a `profiles.yml` file as an inline string. Use [`{{ secret('NAME') }}`](https://kestra.io/docs/concepts/secret) to inject credentials at runtime rather than hardcoding them. For a cleaner separation of configuration from flow logic, store `profiles.yml` as a [namespace file](https://kestra.io/docs/concepts/namespace-files) and reference it with namespace file functions.

## Common properties

All CLI tasks run in a container defined by `containerImage`. Use an image from [ghcr.io/kestra-io/docker-dbt](https://github.com/kestra-io/docker-dbt) that matches your adapter — `dbt-postgres`, `dbt-bigquery`, `dbt-snowflake`, `dbt-duckdb`, and others are available. The `taskRunner` property defaults to Docker and can be overridden for other execution environments.

## Tasks

`DbtCLI` is the primary task and runs any dbt CLI command. For state-based selection across runs, `storeManifest` and `loadManifest` persist `manifest.json` to and from the Kestra KV Store. Dedicated per-command tasks (`Run`, `Test`, `Seed`, `Build`, and others) also exist but are **deprecated** — use `DbtCLI` with the corresponding dbt command (for example `commands: ["dbt build"]`) instead.

For dbt Cloud, use `TriggerRun` — it starts a job and waits for completion by default without requiring a container. Use `CheckStatus` to read a run that was triggered outside of Kestra. Give it a `runId` for one specific run, or a `jobId` or `environmentId` to resolve the most recent successful run of that job or environment, which keeps asset lineage fresh for runs dbt Cloud started on its own schedule. When resolving that way it records the run it read, so a scheduled refresh does not re-emit the same lineage on every tick, and `failOnUnsuccessful: false` lets it read a run that failed without failing the flow. Assets from a dbt Cloud run carry the producing job as metadata: `dbtCloudJobId`, `dbtCloudJobName`, `dbtCloudJobSchedule` with the job's cron, and `dbtCloudJobScheduled` telling you whether its schedule trigger is on. Read the flag before treating the cron as a cadence, since dbt Cloud keeps the cron on a job whose schedule is disabled. Asset emission requires `assets.enableAuto: true` on the task: it is off by default and a disabled emitter drops every asset silently. Authenticate dbt Cloud tasks with `accountId` (your numeric dbt Cloud account ID) and `token` (a dbt Cloud API token, supplied via `{{ secret('NAME') }}`). Set `reattach: true` on `TriggerRun` so a worker restart or task retry resumes the run it already started instead of triggering a duplicate. It only reattaches to a run this execution started, so a run triggered elsewhere (a schedule or a manual run) is never picked up.
