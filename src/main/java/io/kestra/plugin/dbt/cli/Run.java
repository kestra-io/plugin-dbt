package io.kestra.plugin.dbt.cli;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Invoke dbt run command."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            title = "Invoke dbt `run` command.",
            code = """
                id: dbt_run
                namespace: company.team

                tasks:
                  - id: working_directory
                    type: io.kestra.plugin.core.flow.WorkingDirectory
                    tasks:
                      - id: clone_repository
                        type: io.kestra.plugin.git.Clone
                        url: https://github.com/kestra-io/dbt-demo
                        branch: main

                      - id: dbt_run
                        type: io.kestra.plugin.dbt.cli.Run
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
public class Run extends AbstractRun {
    @Override
    protected String dbtCommand() {
        return "run";
    }
}
