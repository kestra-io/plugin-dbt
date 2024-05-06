package io.kestra.plugin.dbt.cli;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.runners.ScriptService;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.plugin.dbt.ResultParser;
import io.kestra.plugin.scripts.exec.AbstractExecScript;
import io.kestra.plugin.scripts.exec.scripts.models.DockerOptions;
import io.kestra.plugin.scripts.exec.scripts.models.ScriptOutput;
import io.kestra.plugin.scripts.exec.scripts.runners.AbstractLogConsumer;
import io.kestra.plugin.scripts.exec.scripts.runners.CommandsWrapper;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Execute dbt CLI commands."
)
@Plugin(
    examples = {
        @Example(
            title = "Launch a `dbt build` command on a sample dbt project hosted on GitHub.",
            full = true,
            code = """
                id: dbt_build
                namespace: dev

                tasks:
                  - id: dbt
                    type: io.kestra.core.tasks.flows.WorkingDirectory
                    tasks:
                      - id: cloneRepository
                        type: io.kestra.plugin.git.Clone
                        url: https://github.com/kestra-io/dbt-example
                        branch: main

                      - id: dbt-build
                        type: io.kestra.plugin.dbt.cli.DbtCLI
                        containerImage: ghcr.io/kestra-io/dbt-duckdb:latest
                        taskRunner:
                          type: io.kestra.plugin.scripts.runner.docker.DockerTaskRunner
                        commands:
                          - dbt build
                        profiles: |
                          my_dbt_project:
                            outputs:
                              dev:
                                type: duckdb
                                path: ":memory:"
                            target: dev"""
        ),
        @Example(
            title = "Install a custom dbt version and run `dbt deps` and `dbt build` commands. Note how you can also configure the memory limit for the Docker runner. This is useful when you see Zombie processes.",
            full = true,
            code = """
            id: dbt_custom_dependencies
            namespace: dev

            inputs:
              - id: dbt_version
                type: STRING
                defaults: "dbt-duckdb==1.6.0"

            tasks:
              - id: git
                type: io.kestra.core.tasks.flows.WorkingDirectory
                tasks:
                  - id: clone_repository
                    type: io.kestra.plugin.git.Clone
                    url: https://github.com/kestra-io/dbt-example
                    branch: main

                  - id: dbt
                    type: io.kestra.plugin.dbt.cli.DbtCLI
                    runner: DOCKER
                    docker:
                      image: python:3.11-slim
                      memory:
                        memory: 1GB          
                    beforeCommands:
                      - pip install uv
                      - uv venv --quiet
                      - . .venv/bin/activate --quiet
                      - uv pip install --quiet {{ inputs.dbt_version }}
                    commands:
                      - dbt deps
                      - dbt build
                    profiles: |
                      my_dbt_project:
                        outputs:
                          dev:
                            type: duckdb
                            path: ":memory:"
                            fixed_retries: 1
                            threads: 16
                            timeout_seconds: 300
                        target: dev"""
        )        
    }
)
public class DbtCLI extends AbstractExecScript {
    private static final ObjectMapper MAPPER = JacksonMapper.ofYaml();
    private static final String DEFAULT_IMAGE = "ghcr.io/kestra-io/dbt";

    @Schema(
        title = "The list of dbt CLI commands to run."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    @NotEmpty
    private List<String> commands;

    @Schema(
        title = "The `profiles.yml` file content.",
        description = "If a `profile.yml` file already exists in the current working directory, it will be overridden."
    )
    @PluginProperty(dynamic = true)
    private String profiles;

    @Schema(
        title = "The dbt project directory, if it's not the working directory.",
        description = "To use it, also use this directory in the `--project-dir` flag on the dbt CLI commands."
    )
    @PluginProperty(dynamic = true)
    private String projectDir;

    @Schema(
        title = "Docker options for the `DOCKER` runner.",
        defaultValue = "{image=" + DEFAULT_IMAGE + ", pullPolicy=ALWAYS}"
    )
    @PluginProperty
    @Builder.Default
    private DockerOptions docker = DockerOptions.builder().build();

    @Builder.Default
    @Schema(
        title = "Parse run result.",
        description = "Parsing run result to display duration of each task inside dbt."
    )
    @PluginProperty
    protected Boolean parseRunResults = true;

    @Builder.Default
    protected String containerImage = DEFAULT_IMAGE;

    @Override
    protected DockerOptions injectDefaults(DockerOptions original) {
        var builder = original.toBuilder();
        if (original.getImage() == null) {
            builder.image(DEFAULT_IMAGE);
        }
        if (original.getEntryPoint() == null) {
            builder.entryPoint(Collections.emptyList());
        }

        return builder.build();
    }

    @Override
    public ScriptOutput run(RunContext runContext) throws Exception {
        CommandsWrapper commands = this.commands(runContext)
            .withLogConsumer(new AbstractLogConsumer() {
                @Override
                public void accept(String line, Boolean isStdErr) {
                    LogService.parse(runContext, line);
                }
            });

        Path workingDirectory = projectDir == null ? commands.getWorkingDirectory() : commands.getWorkingDirectory().resolve(projectDir);

        if (profiles != null && !profiles.isEmpty()) {
            if (Files.exists(Path.of(".profiles/profiles.yml"))) {
                runContext.logger().warn("A 'profiles.yml' file already exist in the task working directory, it will be overridden.");
            }

            FileUtils.writeStringToFile(
                new File(workingDirectory.toString(), "profiles.yml"),
                runContext.render(profiles),
                StandardCharsets.UTF_8
            );
        }

        List<String> commandsArgs = ScriptService.scriptCommands(
            this.interpreter,
            this.getBeforeCommandsWithOptions(),
            this.commands
        );

        ScriptOutput run = this.commands(runContext)
            .addEnv(Map.of(
                "PYTHONUNBUFFERED", "true",
                "PIP_ROOT_USER_ACTION", "ignore"
            ))
            .withCommands(commandsArgs)
            .run();

        if (this.parseRunResults && workingDirectory.resolve("target/run_results.json").toFile().exists()) {
            URI results = ResultParser.parseRunResult(runContext, workingDirectory.resolve("target/run_results.json").toFile());
            run.getOutputFiles().put("run_results.json", results);
        }

        if (workingDirectory.resolve("target/manifest.json").toFile().exists()) {
            URI manifest = ResultParser.parseManifest(runContext, workingDirectory.resolve("target/manifest.json").toFile());
            run.getOutputFiles().put("manifest.json", manifest);
        }

        return run;
    }
}
