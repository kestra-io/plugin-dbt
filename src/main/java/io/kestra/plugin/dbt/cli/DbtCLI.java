package io.kestra.plugin.dbt.cli;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.exceptions.ResourceExpiredException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.runners.AbstractLogConsumer;
import io.kestra.core.models.tasks.runners.ScriptService;
import io.kestra.core.models.tasks.runners.TaskRunner;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.storages.kv.KVStore;
import io.kestra.core.storages.kv.KVValue;
import io.kestra.core.storages.kv.KVValueAndMetadata;
import io.kestra.plugin.dbt.ResultParser;
import io.kestra.plugin.scripts.exec.AbstractExecScript;
import io.kestra.plugin.scripts.exec.scripts.models.DockerOptions;
import io.kestra.plugin.scripts.exec.scripts.models.ScriptOutput;
import io.kestra.plugin.scripts.exec.scripts.runners.CommandsWrapper;
import io.kestra.plugin.scripts.runner.docker.Docker;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.Valid;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import org.apache.commons.lang3.StringUtils;

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
                namespace: company.team

                tasks:
                  - id: dbt
                    type: io.kestra.plugin.core.flow.WorkingDirectory
                    tasks:
                      - id: cloneRepository
                        type: io.kestra.plugin.git.Clone
                        url: https://github.com/kestra-io/dbt-example
                        branch: main

                      - id: dbt-build
                        type: io.kestra.plugin.dbt.cli.DbtCLI
                        containerImage: ghcr.io/kestra-io/dbt-duckdb:latest
                        taskRunner:
                          type: io.kestra.plugin.scripts.runner.docker.Docker
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
            namespace: company.team

            inputs:
              - id: dbt_version
                type: STRING
                defaults: "dbt-duckdb==1.6.0"

            tasks:
              - id: git
                type: io.kestra.plugin.core.flow.WorkingDirectory
                tasks:
                  - id: clone_repository
                    type: io.kestra.plugin.git.Clone
                    url: https://github.com/kestra-io/dbt-example
                    branch: main

                  - id: dbt
                    type: io.kestra.plugin.dbt.cli.DbtCLI
                    taskRunner:
                      type: io.kestra.plugin.scripts.runner.docker.Docker
                      memory:
                        memory: 1GB          
                    containerImage: python:3.11-slim
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
        ),
        @Example(
            title = "Clone a [Git repository](https://github.com/kestra-io/dbt-example) and build dbt models. Note that, as the dbt project files are in a separate directory, you need to set the `projectDir` task property and use `--project-dir` in each dbt CLI command.",
            full = true,
            code = """
                id: dwh_and_analytics
                namespace: company.team
                
                tasks:
                  - id: dbt
                    type: io.kestra.plugin.core.flow.WorkingDirectory
                    tasks:
                    - id: clone_repository
                      type: io.kestra.plugin.git.Clone
                      url: https://github.com/kestra-io/dbt-example
                      branch: master
                
                    - id: dbt_build
                      type: io.kestra.plugin.dbt.cli.DbtCLI
                      taskRunner:
                        type: io.kestra.plugin.scripts.runner.docker.Docker
                      containerImage: ghcr.io/kestra-io/dbt-duckdb:latest
                      commands:
                        - dbt deps --project-dir dbt --target prod
                        - dbt build --project-dir dbt --target prod
                      projectDir: dbt
                      profiles: |
                        my_dbt_project:
                          outputs:
                            dev:
                              type: duckdb
                              path: dbt.duckdb
                              extensions:
                                - parquet
                              fixed_retries: 1
                              threads: 16
                              timeout_seconds: 300
                            prod:
                              type: duckdb
                              path: dbt2.duckdb
                              extensions:\s
                                - parquet
                              fixed_retries: 1
                              threads: 16
                              timeout_seconds: 300
                          target: dev"""
        ),
        @Example(
            title = "Clone a [Git repository](https://github.com/kestra-io/dbt-example) and build dbt models in defer mode. The loadManifest property will fetch an existing manifest.json and use it for the defer build.",
            full = true,
            code = """
                id: dbt_duckdb
                namespace: company.team
                tasks:
                  - id: dbt
                    type: io.kestra.plugin.core.flow.WorkingDirectory
                    tasks:
                      - id: clone_repository
                        type: io.kestra.plugin.git.Clone
                        url: https://github.com/kestra-io/dbt-example
                        branch: main
                
                      - id: dbt_build
                        type: io.kestra.plugin.dbt.cli.DbtCLI
                        taskRunner:
                          type: io.kestra.plugin.scripts.runner.docker.Docker
                          delete: true
                        containerImage: ghcr.io/kestra-io/dbt-duckdb:latest
                        loadManifest:
                          key: manifest.json
                          namespace: company.team
                        storeManifest:
                          key: manifest.json
                          namespace: company.team
                        commands:
                          - dbt build --defer --state ./target --target prod
                
                        profiles: |
                          my_dbt_project:
                            outputs:
                              dev:
                                type: duckdb
                                path: ":memory:"
                                fixed_retries: 1
                                threads: 16
                                timeout_seconds: 300
                              prod:
                                type: duckdb
                                path: dbt2.duckdb
                                extensions:
                                  - parquet
                                fixed_retries: 1
                                threads: 16
                                timeout_seconds: 300
                            target: dev
                """
        )
    }
)
public class DbtCLI extends AbstractExecScript {
    private static final ObjectMapper MAPPER = JacksonMapper.ofYaml();
    private static final String DEFAULT_IMAGE = "ghcr.io/kestra-io/dbt";

    @Schema(
        title = "The list of dbt CLI commands to run."
    )
    @NotNull
    @NotEmpty
    @PluginProperty(dynamic = true)
    private List<String> commands;

    @Schema(
        title = "The `profiles.yml` file content.",
        description = "If a `profile.yml` file already exists in the current working directory, it will be overridden."
    )
    private Property<String> profiles;

    @Schema(
        title = "The dbt project directory, if it's not the working directory.",
        description = "To use it, also use this directory in the `--project-dir` flag on the dbt CLI commands."
    )
    private Property<String> projectDir;

    @Builder.Default
    @Schema(
        title = "Parse run result.",
        description = "Parsing run result to display duration of each task inside dbt."
    )
    protected Property<Boolean> parseRunResults = Property.of(Boolean.TRUE);

    @Schema(
        title = "The task runner to use.",
        description = """
            Task runners are provided by plugins, each have their own properties.
            If you change from the default one, be careful to also configure the entrypoint to an empty list if needed."""
    )
    @PluginProperty
    @Builder.Default
    @Valid
    protected TaskRunner taskRunner = Docker.builder()
        .type(Docker.class.getName())
        .entryPoint(new ArrayList<>())
        .build();

    @Builder.Default
    protected String containerImage = DEFAULT_IMAGE;

    @Schema(
        title = "Store manifest.",
        description = "Use this field to persist your manifest.json in the KV Store."
    )
    protected KvStoreManifest storeManifest;

    @Schema(
        title = "Load manifest.",
        description = """
            Use this field to retrieve an existing manifest.json in the KV Store and put it in the inputFiles.
            The manifest.json will be put under ./target/manifest.json or under ./projectDir/target/manifest.json if you specify a projectDir.
            """
    )
    protected KvStoreManifest loadManifest;

    @Override
    protected DockerOptions injectDefaults(DockerOptions original) {
        if (original == null) {
            return null;
        }

        var builder = original.toBuilder();
        if (original.getImage() == null) {
            builder.image(this.getContainerImage());
        }
        if (original.getEntryPoint() == null) {
            builder.entryPoint(new ArrayList<>());
        }

        return builder.build();
    }

    @Override
    public ScriptOutput run(RunContext runContext) throws Exception {
        KVStore storeManifestKvStore = null;

        //Check/fail if a KV store exists with given namespace
        if(this.getStoreManifest() != null) {
            storeManifestKvStore = runContext.namespaceKv(this.getStoreManifest().getNamespace().as(runContext, String.class));
        }

        CommandsWrapper commands = this.commands(runContext)
            .withEnableOutputDirectory(true) // force the output dir, so we can get the run_results.json and manifest.json files on each task runners
            .withLogConsumer(new AbstractLogConsumer() {
                @Override
                public void accept(String line, Boolean isStdErr) {
                    LogService.parse(runContext, line);
                }
            });

        Path projectWorkingDirectory = projectDir == null ? commands.getWorkingDirectory() : commands.getWorkingDirectory().resolve(projectDir.as(runContext, String.class));

        //Load manifest from KV store
        if(this.getLoadManifest() != null) {
            KVStore loadManifestKvStore = runContext.namespaceKv(this.getLoadManifest().getNamespace().as(runContext, String.class));
            fetchAndStoreManifestIfExists(runContext, loadManifestKvStore, projectWorkingDirectory);
        }

        //Create profiles.yml
        String profilesString = profiles == null ? null : profiles.as(runContext, String.class);
        if (profilesString != null && !profilesString.isEmpty()) {
            var profileFile = new File(commands.getWorkingDirectory().toString(), "profiles.yml");
            if (profileFile.exists()) {
                runContext.logger().info("A 'profiles.yml' file already exist in the task working directory, it will be overridden.");
            }

            FileUtils.writeStringToFile(
                profileFile,
                profilesString,
                StandardCharsets.UTF_8
            );
        }

        //Create and run commands
        List<String> commandsArgs = ScriptService.scriptCommands(
            this.interpreter,
            this.getBeforeCommandsWithOptions(),
            runContext.render(this.commands).stream().map(command -> command.concat(" --log-format json")).toList()
        );

        // check that if a command uses --project-dir, the projectDir must be set
        if (commandsArgs.stream().anyMatch(cmd -> cmd.contains("--project-dir")) && this.projectDir == null) {
            runContext.logger().warn("One of the dbt CLI commands uses the `--project-dir` flag, but the `projectDir` task property is not set. Make sure to set the `projectDir` property.");
        }

        ScriptOutput run = commands
            .addEnv(Map.of(
                "PYTHONUNBUFFERED", "true",
                "PIP_ROOT_USER_ACTION", "ignore"
            ))
            .withCommands(commandsArgs)
            .run();

        //Parse run results
        if (this.parseRunResults.as(runContext, Boolean.class) && projectWorkingDirectory.resolve("target/run_results.json").toFile().exists()) {
            URI results = ResultParser.parseRunResult(runContext, projectWorkingDirectory.resolve("target/run_results.json").toFile());
            run.getOutputFiles().put("run_results.json", results);
        }

        File manifestFile = projectWorkingDirectory.resolve("target/manifest.json").toFile();
        if (manifestFile.exists()) {
            if(this.getStoreManifest() != null) {
                final String key = this.getStoreManifest().getKey().as(runContext, String.class);
                storeManifestKvStore.put(key, new KVValueAndMetadata(null, JacksonMapper.toObject(Files.readString(manifestFile.toPath()))));
            }

            URI manifest = ResultParser.parseManifest(runContext, manifestFile);
            run.getOutputFiles().put("manifest.json", manifest);

        }

        return run;
    }

    private void fetchAndStoreManifestIfExists(RunContext runContext, KVStore loadManifestKvStore, Path projectWorkingDirectory) throws IOException, ResourceExpiredException, IllegalVariableEvaluationException {
        Optional<KVValue> manifestValue = loadManifestKvStore.getValue(this.getLoadManifest().getKey().as(runContext, String.class));

        if(manifestValue.isEmpty() || manifestValue.get().value() == null || StringUtils.isBlank(manifestValue.get().value().toString())) {
            runContext.logger().warn("Property `loadManifest` has been used but no manifest has been found in the KV Store.");
            return;
        }
        var manifestFile = new File(projectWorkingDirectory.toString(), "target/manifest.json");
        FileUtils.writeStringToFile(
            manifestFile,
            JacksonMapper.ofJson()
                .writeValueAsString(manifestValue.get().value()),
            StandardCharsets.UTF_8
        );
    }

    @Builder
    @Getter
    public static class KvStoreManifest {
        @NotNull
        @Schema(title = "Key", description = "KV store key containing the manifest.json")
        Property<String> key;

        @NotNull
        @Schema(title = "Namespace", description = "KV store namespace containing the manifest.json")
        Property<String> namespace;
    }
}
