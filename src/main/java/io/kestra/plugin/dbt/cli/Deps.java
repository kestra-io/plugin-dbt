package io.kestra.plugin.dbt.cli;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.scripts.exec.scripts.models.ScriptOutput;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import java.nio.file.Path;


@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Invoke dbt deps command (Deprecated).",
    description = "This task is deprecated, please use the [io.kestra.plugin.dbt.cli.DbtCLI](https://kestra.io/plugins/tasks/io.kestra.plugin.dbt.cli.DbtCLI) task instead.",
    deprecated = true
)
@Plugin(
    examples = {
        @Example(
            full = true,
            title = "Invoke dbt `deps` command",
            code = """
                id: dbt_deps
                namespace: company.team

                tasks:
                  - id: working_directory
                    type: io.kestra.plugin.core.flow.WorkingDirectory
                    tasks:
                      - id: clone_repository
                        type: io.kestra.plugin.git.Clone
                        url: https://github.com/kestra-io/dbt-demo
                        branch: main

                      - id: dbt_deps
                        type: io.kestra.plugin.dbt.cli.Deps
                        taskRunner:
                          type: io.kestra.plugin.scripts.runner.docker.Docker
                        dbtPath: /usr/local/bin/dbt
                        containerImage: ghcr.io/kestra-io/dbt-duckdb
                        profiles: |
                          jaffle_shop:
                            outputs:
                              dev:
                                type: duckdb
                                path: ':memory:'
                                extensions:
                                  - parquet
                            target: dev
                """
        )
    }
)
@Deprecated
public class Deps extends AbstractRun {
    @Override
    protected String dbtCommand() {
        return "deps";
    }

    @Override
    protected void parseResults(RunContext runContext, Path workingDirectory, ScriptOutput scriptOutput) {
        // 'dbt deps' didn't return any result files.
    }
}
