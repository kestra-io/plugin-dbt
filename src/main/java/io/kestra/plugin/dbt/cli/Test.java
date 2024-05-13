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
    title = "Invoke dbt `test` command."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            title = "Invoke dbt `test` command.",
            code = """
                id: dbt_test
                namespace: dev
                tasks:
                  - id: wdir
                    type: io.kestra.core.tasks.flows.WorkingDirectory
                    tasks:
                    - id: clone_repository
                      type: io.kestra.plugin.git.Clone
                      url: https://github.com/kestra-io/dbt-example
                      branch: main
                    - id: dbt_test
                      type: io.kestra.plugin.dbt.cli.Test
                      taskRunner:
                        type: io.kestra.plugin.scripts.runner.docker.DockerTaskRunner
                      dbtPath: /usr/local/bin/dbt
                      containerImage: ghcr.io/kestra-io/dbt-duckdb
                      profiles: |
                        my_dbt_project:
                          outputs:
                            dev:
                              type: duckdb
                              path: ':memory:'
                          target: dev
                """
        )
    }
)
public class Test extends AbstractRun {
    @Override
    protected String dbtCommand() {
        return "test";
    }
}
