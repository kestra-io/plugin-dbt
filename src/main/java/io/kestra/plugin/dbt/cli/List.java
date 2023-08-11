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
    title = "Invoke dbt `list` command"
)
@Plugin(
    examples = {
        @Example(
            full = true,
            title = "Invoke dbt `list` command",
            code = """
                namespace: io.kestra.tests
                id: dbt-list
                tasks:
                  - id: working-directory
                    type: io.kestra.core.tasks.flows.WorkingDirectory
                    tasks:
                    - id: cloneRepository
                      type: io.kestra.plugin.git.Clone
                      url: https://github.com/kestra-io/dbt-demo
                      branch: main
                    - id: dbt-list
                      type: io.kestra.plugin.dbt.cli.List
                      runner: DOCKER
                      dbtPath: /usr/local/bin/dbt
                      docker:
                        image: ghcr.io/kestra-io/dbt-bigquery:latest
                      profiles: |
                        jaffle_shop:
                          outputs:
                            dev:
                              type: bigquery
                              dataset: dwh
                              fixed_retries: 1
                              keyfile: sa.json
                              location: EU
                              method: service-account
                              priority: interactive
                              project: my-project
                              threads: 8
                              timeout_seconds: 300
                          target: dev
                      inputFiles:
                        sa.json: "{{ secret('GCP_CREDS') }}"
                """
        )
    }
)
public class List extends AbstractRun {
    @Override
    protected String dbtCommand() {
        return "list";
    }

    @Override
    protected void parseResults(RunContext runContext, Path workingDirectory, ScriptOutput scriptOutput) {
        // 'dbt list' didn't return any result files.
    }
}
