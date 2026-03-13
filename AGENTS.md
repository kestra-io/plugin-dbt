# Kestra dbt Plugin

## What

Integrate dbt data transformations into Kestra orchestration pipelines. Exposes 13 plugin components (tasks, triggers, and/or conditions).

## Why

Enables Kestra workflows to interact with DBT, allowing orchestration of DBT-based operations as part of data pipelines and automation workflows.

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

### Important Commands

```bash
# Build the plugin
./gradlew shadowJar

# Run tests
./gradlew test

# Build without tests
./gradlew shadowJar -x test
```

### Configuration

All tasks and triggers accept standard Kestra plugin properties. Credentials should use
`{{ secret('SECRET_NAME') }}` — never hardcode real values.

## Agents

**IMPORTANT:** This is a Kestra plugin repository (prefixed by `plugin-`, `storage-`, or `secret-`). You **MUST** delegate all coding tasks to the `kestra-plugin-developer` agent. Do NOT implement code changes directly — always use this agent.
