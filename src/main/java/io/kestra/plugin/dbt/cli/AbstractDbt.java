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
import io.kestra.plugin.dbt.ResultParser;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractDbt extends Task implements RunnableTask<ScriptOutput>, NamespaceFilesInterface, InputFilesInterface, OutputFilesInterface {
    private static final String DEFAULT_IMAGE = "ghcr.io/kestra-io/dbt";

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
    protected TaskRunner taskRunner = Docker.builder()
            .type(Docker.class.getName())
            .entryPoint(Collections.emptyList())
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

    private List<String> outputFiles;

    protected abstract java.util.List<String> dbtCommands(RunContext runContext) throws IllegalVariableEvaluationException;

    @Override
    public ScriptOutput run(RunContext runContext) throws Exception {
        CommandsWrapper commandsWrapper = new CommandsWrapper(runContext)
            .withEnv(this.getEnv() != null ? this.getEnv().asMap(runContext, String.class, String.class) : Collections.emptyMap())
            .withNamespaceFiles(namespaceFiles)
            .withInputFiles(inputFiles)
            .withOutputFiles(outputFiles)
            .withRunnerType(this.getRunner() != null ? this.getRunner().as(runContext, RunnerType.class) : null)
            .withDockerOptions(this.getDocker() != null ? this.getDocker().as(runContext, DockerOptions.class) : null)
            .withContainerImage(this.containerImage.as(runContext, String.class))
            .withTaskRunner(this.taskRunner)
            .withLogConsumer(new AbstractLogConsumer() {
                @Override
                public void accept(String line, Boolean isStdErr) {
                    LogService.parse(runContext, line);
                }
            })
            .withEnableOutputDirectory(true); //force output files on task runners
        Path workingDirectory = commandsWrapper.getWorkingDirectory();

        String profileString = profiles != null ? profiles.as(runContext, String.class) : null;
        if (profileString != null && !profileString.isEmpty()) {
            if (Files.exists(Path.of(".profiles/profiles.yml"))) {
                runContext.logger().warn("A 'profiles.yml' file already exist in the task working directory, it will be overridden.");
            }

            FileUtils.writeStringToFile(
                new File(workingDirectory.resolve(".profile").toString(), "profiles.yml"),
                    profileString,
                StandardCharsets.UTF_8
            );
        }

        List<String> commandsArgs = ScriptService.scriptCommands(
            List.of("/bin/sh", "-c"),
            null,
            List.of(createDbtCommand(runContext))
        );

        ScriptOutput run = commandsWrapper
            .addEnv(Map.of(
                "PYTHONUNBUFFERED", "true",
                "PIP_ROOT_USER_ACTION", "ignore"
            ))
            .withCommands(commandsArgs)
            .run();

        parseResults(runContext, workingDirectory, run);

        return run;
    }

    private String createDbtCommand(RunContext runContext) throws IllegalVariableEvaluationException {
        List<String> commands = new ArrayList<>(List.of(
            dbtPath.as(runContext, String.class),
            "--log-format json"
        ));

        if (Boolean.TRUE.equals(this.debug.as(runContext, Boolean.class))) {
            commands.add("--debug");
        }

        if (Boolean.TRUE.equals(this.failFast.as(runContext, Boolean.class))) {
            commands.add("--fail-fast");
        }

        if (Boolean.TRUE.equals(this.warnError.as(runContext, Boolean.class))) {
            commands.add("--warn-error");
        }

        commands.addAll(dbtCommands(runContext));

        if (this.projectDir != null) {
            commands.add("--project-dir {{" + ScriptService.VAR_WORKING_DIR + "}}" + this.projectDir.as(runContext, String.class));
        } else {
            commands.add("--project-dir {{" + ScriptService.VAR_WORKING_DIR + "}}");
        }

        return String.join(" ", commands);
    }

    protected void parseResults(RunContext runContext, Path workingDirectory, ScriptOutput scriptOutput) throws IllegalVariableEvaluationException, IOException {
        String baseDir = this.projectDir != null ? this.projectDir.as(runContext, String.class) : "";

        File runResults = workingDirectory.resolve(baseDir + "target/run_results.json").toFile();

        if (this.parseRunResults.as(runContext, Boolean.class) && runResults.exists()) {
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
