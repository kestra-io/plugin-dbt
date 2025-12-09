package io.kestra.plugin.dbt.cli;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.exceptions.ResourceExpiredException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.flows.State;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.RunnableTaskException;
import io.kestra.core.models.tasks.runners.AbstractLogConsumer;
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
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.*;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

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
                            target: dev
                """
        ),
        @Example(
            title = "Sync dbt project files from a specific GitHub branch to Kestra's [Namespace Files](https://kestra.io/docs/concepts/namespace-files) and run `dbt build` command. Note that we `exclude` the `profiles.yml` file because the `profiles` is defined in the dbt task directly. This `exclude` pattern is useful if you want to override the `profiles.yml` file by defining it in the dbt task. In this example, the `profiles.yml` was [initially targeting](https://github.com/kestra-io/dbt-example/blob/master/dbt/profiles.yml) a `dev` environment, but we override it to target a `prod` environment.",
            full = true,
            code = """
                id: dbt_build
                namespace: company.team

                tasks:
                  - id: sync
                    type: io.kestra.plugin.git.SyncNamespaceFiles
                    url: https://github.com/kestra-io/dbt-example
                    branch: master
                    namespace: "{{ flow.namespace }}"
                    gitDirectory: dbt
                    dryRun: false

                  - id: dbt_build
                    type: io.kestra.plugin.dbt.cli.DbtCLI
                    containerImage: ghcr.io/kestra-io/dbt-duckdb:latest
                    namespaceFiles:
                      enabled: true
                      exclude:
                        - profiles.yml
                    taskRunner:
                      type: io.kestra.plugin.scripts.runner.docker.Docker
                    commands:
                      - dbt build
                    profiles: |
                      my_dbt_project:
                        outputs:
                          prod:
                            type: duckdb
                            path: ":memory:"
                            schema: main
                            threads: 8
                        target: prod
                """
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
                            target: dev
                """
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
                        - dbt deps --target prod
                        - dbt build --target prod
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
                              extensions:
                                - parquet
                              fixed_retries: 1
                              threads: 16
                              timeout_seconds: 300
                          target: dev
                """
        ),
        @Example(
            title = "Clone a [Git repository](https://github.com/kestra-io/dbt-example) and build dbt models using the `--defer` flag. The `loadManifest` property will fetch an existing `manifest.json` and use it to run a subset of models that have changed since the last run.",
            full = true,
            code = """
                id: dbt_defer
                namespace: company.team
                inputs:
                  - id: dbt_command
                    type: SELECT
                    allowCustomValue: true
                    defaults: dbt build --project-dir dbt --target prod --no-partial-parse
                    values:
                      - dbt build --target prod --no-partial-parse
                      - dbt build --target prod --no-partial-parse --select state:modified+ --defer --state ./target

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
                          delete: true
                        containerImage: ghcr.io/kestra-io/dbt-duckdb:latest
                        loadManifest:
                          key: manifest.json
                          namespace: "{{ flow.namespace }}"
                        storeManifest:
                          key: manifest.json
                          namespace: "{{ flow.namespace }}"
                        projectDir: dbt
                        commands:
                          - "{{ inputs.dbt_command }}"
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
    },
    metrics = {
        @Metric(
            name = "log.stats.success",
            type = Counter.TYPE,
            unit = "records",
            description = "The number of successful log entries parsed from DBT output."
        ),
        @Metric(
            name = "log.stats.warn",
            type = Counter.TYPE,
            unit = "records",
            description = "The number of warning log entries parsed from DBT output."
        ),
        @Metric(
            name = "log.stats.error",
            type = Counter.TYPE,
            unit = "records",
            description = "The number of error log entries parsed from DBT output."
        )
    }
)
public class DbtCLI extends AbstractExecScript implements RunnableTask<DbtCLI.Output> {
    private static final ObjectMapper MAPPER = JacksonMapper.ofYaml();
    private static final String CORE_IMAGE = "ghcr.io/kestra-io/dbt";
    private static final String FUSION_IMAGE = "ghcr.io/kestra-io/dbt-fusion";

    @Schema(
        title = "The list of dbt CLI commands to run."
    )
    @NotNull
    private Property<List<String>> commands;

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
    protected Property<Boolean> parseRunResults = Property.ofValue(Boolean.TRUE);

    @Schema(
        title = "The task runner to use.",
        description = """
            Task runners are provided by plugins, each have their own properties.
            If you change from the default one, be careful to also configure the entrypoint to an empty list if needed."""
    )
    @PluginProperty
    @Builder.Default
    @Valid
    protected TaskRunner<?> taskRunner = Docker.builder()
        .type(Docker.class.getName())
        .entryPoint(new ArrayList<>())
        .build();

    @Builder.Default
    protected Property<String> containerImage = Property.ofValue(CORE_IMAGE);

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

    @Schema(
        title = "Log format.",
        description = """
            The log format is JSON by default. The format will be applied after all commands like this --log-format <logFormat>.
            The possible values are JSON, DEBUG, TEXT. You can set it to NONE to avoid adding this argument to your commands.
            """
    )
    @Builder.Default
    private Property<LogFormat> logFormat = Property.ofValue(LogFormat.JSON);

    @Schema(
        title = "dbt engine",
        description = """
            Selects the default container image when no explicit image is provided.

            Image resolution priority:
                - If `taskRunner.image` is set, that image is used.
                - Otherwise, if `containerImage` is set on the task, it is used.
                - Otherwise, the `engine` determines the default image:
                   - CORE   → ghcr.io/kestra-io/dbt
                   - FUSION → ghcr.io/kestra-io/dbt-fusion
            """
    )
    @Builder.Default
    private Property<Engine> engine = Property.ofValue(Engine.CORE);

    @Override
    protected DockerOptions injectDefaults(RunContext runContext, DockerOptions original) throws IllegalVariableEvaluationException {
        if (original == null) {
            return null;
        }

        var builder = original.toBuilder();
        if (original.getImage() == null) {
            var rContainerImage = runContext.render(this.containerImage).as(String.class).orElse(null);

            if (rContainerImage != null) {
                builder.image(rContainerImage);
            } else {
                var rEngine = runContext.render(this.engine).as(Engine.class).orElse(Engine.CORE);
                builder.image(rEngine == Engine.FUSION ? FUSION_IMAGE : CORE_IMAGE);
            }
        }

        if (original.getEntryPoint() == null) {
            builder.entryPoint(new ArrayList<>());
        }

        return builder.build();
    }

    @Override
    public Output run(RunContext runContext) throws Exception {

        var logger = runContext.logger();

        KVStore storeManifestKvStore = null;
        AtomicBoolean hasWarning = new AtomicBoolean(false);

        // Check/fail if a KV store exists with given namespace
        if (this.getStoreManifest() != null) {
            storeManifestKvStore = runContext.namespaceKv(runContext.render(this.getStoreManifest().getNamespace()).as(String.class).orElseThrow());
        }

        CommandsWrapper commandsWrapper = this.commands(runContext)
            .withEnableOutputDirectory(true) // force the output dir, so we can get the run_results.json and manifest.json files on each task runners
            .withLogConsumer(new AbstractLogConsumer() {
                @Override
                public void accept(String line, Boolean isStdErr, Instant instant) {
                    LogService.parse(runContext, line, hasWarning);
                }

                @Override
                public void accept(String line, Boolean isStdErr) {
                    LogService.parse(runContext, line, hasWarning);
                }
            });

        var rProjectDir = runContext.render(projectDir).as(String.class);
        Path projectWorkingDirectory = rProjectDir.map(s -> commandsWrapper.getWorkingDirectory().resolve(s)).orElseGet(commandsWrapper::getWorkingDirectory);

        // Load manifest from KV store
        if (this.getLoadManifest() != null) {
            KVStore loadManifestKvStore = runContext.namespaceKv(runContext.render(this.getLoadManifest().getNamespace()).as(String.class).orElseThrow());
            fetchAndStoreManifestIfExists(runContext, loadManifestKvStore, projectWorkingDirectory);
        }

        String profilesString = runContext.render(profiles).as(String.class).orElse(null);
        if (profilesString != null && !profilesString.isEmpty()) {
            var profileFile = new File(commandsWrapper.getWorkingDirectory().toString(), "profiles.yml");
            if (profileFile.exists()) {
                logger.info("A 'profiles.yml' file already exist in the task working directory, it will be overridden.");
            }

            FileUtils.writeStringToFile(
                profileFile,
                profilesString,
                StandardCharsets.UTF_8
            );
        }

        var rCommands = runContext.render(this.commands).asList(String.class);

        LogFormat rLogFormat = runContext.render(this.logFormat).as(LogFormat.class).orElseThrow();

        ScriptOutput runResults;
        try {
            runResults = commandsWrapper
                .addEnv(Map.of(
                    "PYTHONUNBUFFERED", "true",
                    "PIP_ROOT_USER_ACTION", "ignore"
                ))
                .withInterpreter(this.interpreter)
                .withBeforeCommands(this.beforeCommands)
                .withBeforeCommandsWithOptions(true)
                .withCommands(Property.ofValue(
                    rCommands.stream()
                        .map(command -> {
                            if (command.startsWith("dbt") && rProjectDir.orElse(null) != null && !command.contains("--project-dir")) {
                                command = command.concat(" --project-dir " + rProjectDir.get());
                            }

                            if (command.startsWith("dbt") && !LogFormat.NONE.equals(rLogFormat)) {
                                return command.concat(" --log-format " + rLogFormat.toString().toLowerCase());
                            }
                            return command;
                        })
                        .toList())
                )
                .run();
        } catch (Exception e) {
            runResults = (e instanceof RunnableTaskException rte && rte.getOutput() instanceof ScriptOutput so)
                ? so
                : ScriptOutput.builder().exitCode(1).outputFiles(new HashMap<>()).build();

            parseRunResults(runContext, projectWorkingDirectory, runResults, storeManifestKvStore);
            Output dbtOutput = Output.builder()
                .warningDetected(hasWarning.get())
                .outputFiles(runResults.getOutputFiles())
                .exitCode(runResults.getExitCode())
                .vars(runResults.getVars())
                .build();

            throw new RunnableTaskException(e.getMessage(), dbtOutput);
        }

        parseRunResults(runContext, projectWorkingDirectory, runResults, storeManifestKvStore);

        return Output.builder()
            .warningDetected(hasWarning.get())
            .outputFiles(runResults.getOutputFiles())
            .exitCode(runResults.getExitCode())
            .build();
    }

    private void parseRunResults(RunContext runContext, Path projectWorkingDirectory, ScriptOutput run, KVStore storeManifestKvStore) throws IllegalVariableEvaluationException, IOException {
        if (runContext.render(this.parseRunResults).as(Boolean.class).orElse(Boolean.TRUE) && projectWorkingDirectory.resolve("target/run_results.json").toFile().exists()) {
            URI results = ResultParser.parseRunResult(runContext, projectWorkingDirectory.resolve("target/run_results.json").toFile());
            run.getOutputFiles().put("run_results.json", results);
        }

        File manifestFile = projectWorkingDirectory.resolve("target/manifest.json").toFile();
        if (manifestFile.exists()) {
            if (this.getStoreManifest() != null) {
                final String key = runContext.render(this.getStoreManifest().getKey()).as(String.class).orElseThrow();
                storeManifestKvStore.put(key, new KVValueAndMetadata(null, JacksonMapper.toObject(Files.readString(manifestFile.toPath()))));
            }

            URI manifest = ResultParser.parseManifest(runContext, manifestFile);
            run.getOutputFiles().put("manifest.json", manifest);
        }
    }

    private void fetchAndStoreManifestIfExists(RunContext runContext, KVStore loadManifestKvStore, Path projectWorkingDirectory) throws IOException, ResourceExpiredException, IllegalVariableEvaluationException {
        Optional<KVValue> manifestValue = loadManifestKvStore.getValue(runContext.render(this.getLoadManifest().getKey()).as(String.class).get());

        if (manifestValue.isEmpty() || manifestValue.get().value() == null || StringUtils.isBlank(manifestValue.get().value().toString())) {
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

    @SuperBuilder
    @Getter
    public static class Output extends ScriptOutput {
        @Builder.Default
        private final transient boolean warningDetected = false;

        @Override
        public Optional<State.Type> finalState() {
            return this.warningDetected ? Optional.of(State.Type.WARNING) : Optional.empty();
        }
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

    enum LogFormat {
        JSON,
        TEXT,
        DEBUG,
        NONE
    }

    public enum Engine {
        CORE,
        FUSION
    }
}
