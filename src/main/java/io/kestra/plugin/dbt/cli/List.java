package io.kestra.plugin.dbt.cli;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.runners.RunContext;
import io.kestra.core.tasks.scripts.ScriptOutput;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.AccessLevel;
import lombok.Builder;
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
                        dockerOptions:
                          image: ghcr.io/kestra-io/dbt-bigquery:latest
                        inputFiles:
                          .profile/profiles.yml: |
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
                          sa.json: "{{ secret('GCP_CREDS') }}"
                """
        )
    }
)
public class List extends AbstractRun {
    @Override
    protected String command() {
        return "list";
    }

    @Override
    protected void parseResults(RunContext runContext, ScriptOutput scriptOutput) {
        // 'dbt list' didn't return any result files.
    }
}
