# Kestra dbt Plugin

## What

- Provides plugin components under `io.kestra.plugin.dbt`.
- Includes classes such as `ResultParser`, `RunResult`, `Manifest`, `Seed`.

## Why

- What user problem does this solve? Teams need to integrate dbt data transformations into Kestra orchestration pipelines from orchestrated workflows instead of relying on manual console work, ad hoc scripts, or disconnected schedulers.
- Why would a team adopt this plugin in a workflow? It keeps DBT steps in the same Kestra flow as upstream preparation, approvals, retries, notifications, and downstream systems.
- What operational/business outcome does it enable? It reduces manual handoffs and fragmented tooling while improving reliability, traceability, and delivery speed for processes that depend on DBT.

## How

### Architecture

Single-module plugin. Source packages under `io.kestra.plugin`:

- `dbt`

### Key Plugin Classes

- `io.kestra.plugin.dbt.cli.Build`
- `io.kestra.plugin.dbt.cli.Compile`
- `io.kestra.plugin.dbt.cli.DbtCLI`
- `io.kestra.plugin.dbt.cli.Deps`
- `io.kestra.plugin.dbt.cli.Freshness`
- `io.kestra.plugin.dbt.cli.List`
- `io.kestra.plugin.dbt.cli.Run`
- `io.kestra.plugin.dbt.cli.Seed`
- `io.kestra.plugin.dbt.cli.Setup`
- `io.kestra.plugin.dbt.cli.Snapshot`
- `io.kestra.plugin.dbt.cli.Test`
- `io.kestra.plugin.dbt.cloud.CheckStatus`
- `io.kestra.plugin.dbt.cloud.TriggerRun`

### Project Structure

```
plugin-dbt/
├── src/main/java/io/kestra/plugin/dbt/models/
├── src/test/java/io/kestra/plugin/dbt/models/
├── build.gradle
└── README.md
```

## References

- https://kestra.io/docs/plugin-developer-guide
- https://kestra.io/docs/plugin-developer-guide/contribution-guidelines
