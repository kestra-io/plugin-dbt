package io.kestra.plugin.dbt.cli;

import com.fasterxml.jackson.annotation.JsonSetter;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.*;
import io.kestra.core.models.tasks.runners.AbstractLogConsumer;
import io.kestra.core.models.tasks.runners.ScriptService;
import io.kestra.core.models.tasks.runners.TaskRunner;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.plugin.dbt.ResultParser;
import io.kestra.plugin.dbt.internals.PythonBasedPlugin;
import io.kestra.plugin.dbt.internals.PythonEnvironmentManager;
import io.kestra.plugin.dbt.internals.PythonEnvironmentManager.ResolvedPythonEnvironment;
import io.kestra.plugin.scripts.exec.scripts.models.DockerOptions;
import io.kestra.plugin.scripts.exec.scripts.models.RunnerType;
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
import java.time.Instant;
import java.util.*;
import java.util.List;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractDbt extends Task implements RunnableTask<ScriptOutput>, NamespaceFilesInterface, InputFilesInterface, OutputFilesInterface, PythonBasedPlugin {

    @Builder.Default
    @Schema(
        title = "Stop execution at the first failure."
    )
    Property<Boolean> failFast = Property.of(false);

    @Builder.Default
    @Schema(
        title = "When dbt would normally warn, raise an exception.",
        description = "Examples include --models that selects nothing, deprecations, configurations with no " +
            "associated models, invalid test configurations, and missing sources/refs in tests."
    )
    Property<Boolean> warnError = Property.of(false);

    @Builder.Default
    @Schema(
        title = "Display debug logging during dbt execution.",
        description = "Useful for debugging and making bug reports."
    )
    Property<Boolean> debug = Property.of(false);

    @Schema(
        title = "Which directory to look in for the dbt_project.yml file.",
        description = "Default is the current working directory and its parents."
    )
    Property<String> projectDir;

    @Builder.Default
    @Schema(
        title = "The path to the dbt CLI"
    )
    Property<String> dbtPath = Property.of("./bin/dbt");

    @Schema(
        title = "The `profiles.yml` file content",
        description = "If a `profile.yml` file already exist in the current working directory, it will be overridden."
    )
    Property<String> profiles;

    @Schema(
            title = "The task runner to use.",
            description = """
            Task runners are provided by plugins, each have their own properties.
            If you change from the default one, be careful to also configure the entrypoint to an empty list if needed."""
    )
    @Builder.Default
    @PluginProperty
    @Valid
    protected TaskRunner<?> taskRunner = Docker.builder()
            .type(Docker.class.getName())
            .entryPoint(new ArrayList<>())
            .build();

    @Schema(title = "The task runner container image, only used if the task runner is container-based.")
    @Builder.Default
    protected Property<String>  containerImage = Property.of(DEFAULT_IMAGE);

    @Schema(
        title = "The runner type.",
        description = "Deprecated, use 'taskRunner' instead."
    )
    @Deprecated
    protected Property<RunnerType> runner;

    @Schema(
        title = "Deprecated, use 'taskRunner' instead"
    )
    @Deprecated
    private Property<DockerOptions> docker;

    @Schema(title = "Deprecated, use the `docker` property instead", deprecated = true)
    @Deprecated
    private Property<DockerOptions> dockerOptions;

    @JsonSetter
    public void setDockerOptions(Property<DockerOptions> dockerOptions) {
        this.dockerOptions = dockerOptions;
        this.docker = dockerOptions;
    }

    @Schema(
        title = "Additional environment variables for the current process."
    )
    protected Property<Map<String, String>> env;

    @Builder.Default
    @Schema(
        title = "Parse run result",
        description = "Parsing run result to display duration of each task inside dbt"
    )
    protected Property<Boolean> parseRunResults = Property.of(Boolean.TRUE);

    private NamespaceFiles namespaceFiles;

    private Object inputFiles;

    private Property<List<String>> outputFiles;

    protected Property<List<String>> dependencies;

    protected Property<String> pythonVersion;

    protected Property<Boolean> dependencyCacheEnabled = Property.of(true);

    protected abstract java.util.List<String> dbtCommands(RunContext runContext) throws IllegalVariableEvaluationException;

    @Override
    public ScriptOutput run(RunContext runContext) throws Exception {
        var renderedOutputFiles = runContext.render(this.outputFiles).asList(String.class);
        var renderedEnvMap = runContext.render(this.getEnv()).asMap(String.class, String.class);

        RunnerType runnerType = runContext.render(this.getRunner()).as(RunnerType.class).orElse(null);

        CommandsWrapper commandsWrapper = new CommandsWrapper(runContext)
            .withEnv(renderedEnvMap.isEmpty() ? new HashMap<>() : renderedEnvMap)
            .withNamespaceFiles(namespaceFiles)
            .withInputFiles(inputFiles)
            .withOutputFiles(renderedOutputFiles.isEmpty() ? null : renderedOutputFiles)
            .withRunnerType(runnerType)
            .withDockerOptions(runContext.render(this.getDocker()).as(DockerOptions.class).orElse(null))
            .withContainerImage(runContext.render(this.getContainerImage()).as(String.class).orElseThrow())
            .withTaskRunner(this.taskRunner)
            .withLogConsumer(new AbstractLogConsumer() {
                @Override
                public void accept(String line, Boolean isStdErr, Instant instant) {
                    LogService.parse(runContext, line);
                }
                @Override
                public void accept(String line, Boolean isStdErr) {
                    LogService.parse(runContext, line);
                }
            })
            .withEnableOutputDirectory(true); //force output files on task runners
        Path workingDirectory = commandsWrapper.getWorkingDirectory();

        Optional<String> profileString = runContext.render(profiles).as(String.class);
        if (profileString.isPresent() && !profileString.get().isEmpty()) {
            if (Files.exists(Path.of(".profiles/profiles.yml"))) {
                runContext.logger().warn("A 'profiles.yml' file already exist in the task working directory, it will be overridden.");
            }

            FileUtils.writeStringToFile(
                new File(workingDirectory.resolve(".profile").toString(), "profiles.yml"),
                    profileString.get(),
                StandardCharsets.UTF_8
            );
        }

        PythonEnvironmentManager pythonEnvironmentManager = new PythonEnvironmentManager(runContext, this);
        ResolvedPythonEnvironment pythonEnvironment = pythonEnvironmentManager.setup(containerImage, taskRunner, runnerType);

        Map<String, String> env = new HashMap<>();
        env.put("PYTHONUNBUFFERED", "true");
        env.put("PIP_ROOT_USER_ACTION", "ignore");
        if (pythonEnvironment.packages() != null) {
            env.put("PYTHONPATH", pythonEnvironment.packages().path().toString());
        };
        ScriptOutput run = commandsWrapper
            .addEnv(env)
            .withInterpreter(Property.of(List.of("/bin/sh", "-c")))
            .withCommands(new Property<>(JacksonMapper.ofJson().writeValueAsString(
                List.of(createDbtCommand(runContext)))
            ))
            .run();

        parseResults(runContext, workingDirectory, run);

        // Cache upload
        if (pythonEnvironmentManager.isCacheEnabled() && pythonEnvironment.packages() != null && !pythonEnvironment.cached()) {
            pythonEnvironmentManager.uploadCache(runContext, pythonEnvironment.packages());
        }

        return run;
    }

    private String createDbtCommand(RunContext runContext) throws IllegalVariableEvaluationException {
        List<String> commands = new ArrayList<>(List.of(
            runContext.render(this.dbtPath).as(String.class).orElseThrow(),
            "--log-format json"
        ));

        if (runContext.render(this.debug).as(Boolean.class).orElse(false)) {
            commands.add("--debug");
        }

        if (runContext.render(this.failFast).as(Boolean.class).orElse(false)) {
            commands.add("--fail-fast");
        }

        if (runContext.render(this.warnError).as(Boolean.class).orElse(false)) {
            commands.add("--warn-error");
        }

        commands.addAll(dbtCommands(runContext));

        if (runContext.render(this.projectDir).as(String.class).isPresent()) {
            commands.add("--project-dir {{" + ScriptService.VAR_WORKING_DIR + "}}" + runContext.render(this.projectDir).as(String.class).get());
        } else {
            commands.add("--project-dir {{" + ScriptService.VAR_WORKING_DIR + "}}");
        }

        return String.join(" ", commands);
    }

    protected void parseResults(RunContext runContext, Path workingDirectory, ScriptOutput scriptOutput) throws IllegalVariableEvaluationException, IOException {
        String baseDir = runContext.render(this.projectDir).as(String.class).orElse("");

        File runResults = workingDirectory.resolve(baseDir + "target/run_results.json").toFile();

        if (runContext.render(this.parseRunResults).as(Boolean.class).orElse(true) && runResults.exists()) {
            URI results = ResultParser.parseRunResult(runContext, runResults);
            scriptOutput.getOutputFiles().put("run_results.json", results);
        }

        File manifestFile = workingDirectory.resolve(baseDir + "target/manifest.json").toFile();

        if (manifestFile.exists()) {
            URI manifest = ResultParser.parseManifest(runContext, manifestFile);
            scriptOutput.getOutputFiles().put("manifest.json", manifest);
        }
    }
}
