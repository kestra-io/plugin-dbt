package io.kestra.plugin.dbt.cli;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.runners.RunContext;
import io.kestra.core.tasks.scripts.ScriptOutput;
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
    title = "Invoke dbt `deps` command"
)
@Plugin(
    examples = {
        @Example(
            full = true,
            title = "Invoke dbt `deps` command",
            code = """
                namespace: io.kestra.tests
                id: dbt-build
                tasks:
                  - id: working-directory
                    type: io.kestra.core.tasks.flows.WorkingDirectory
                    tasks:
                    - id: cloneRepository
                      type: io.kestra.plugin.git.Clone
                      url: https://github.com/dbt-labs/jaffle_shop
                      branch: main
                      - id: dbt-deps
                        type: io.kestra.plugin.dbt.cli.Deps
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
public class Deps extends AbstractRun {
    @Override
    protected String command() {
        return "deps";
    }

    @Override
    protected void parseResults(RunContext runContext, ScriptOutput scriptOutput) {
        // 'dbt deps' didn't return any result files.
    }
}
