# Kestra dbt Plugin

## What

- Provides plugin components under `io.kestra.plugin.dbt`.
- Includes classes such as `ResultParser`, `RunResult`, `Manifest`, `Seed`.

## Why

- This plugin integrates Kestra with Dbt CLI.
- It provides tasks that run dbt via the CLI for building, testing, and managing projects.

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
