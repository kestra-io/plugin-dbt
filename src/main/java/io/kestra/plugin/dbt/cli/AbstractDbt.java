package io.kestra.plugin.dbt.cli;

import com.fasterxml.jackson.annotation.JsonSetter;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.*;
import io.kestra.core.models.tasks.runners.AbstractLogConsumer;
import io.kestra.core.models.tasks.runners.ScriptService;
import io.kestra.core.models.tasks.runners.TaskRunner;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.core.runner.Process;
import io.kestra.plugin.dbt.ResultParser;
import io.kestra.plugin.scripts.exec.scripts.models.DockerOptions;
import io.kestra.plugin.scripts.exec.scripts.models.RunnerType;
import io.kestra.plugin.scripts.exec.scripts.models.ScriptOutput;
import io.kestra.plugin.scripts.exec.scripts.runners.CommandsWrapper;
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
    @PluginProperty
    Boolean failFast = false;

    @Builder.Default
    @Schema(
        title = "When dbt would normally warn, raise an exception.",
        description = "Examples include --models that selects nothing, deprecations, configurations with no " +
            "associated models, invalid test configurations, and missing sources/refs in tests."
    )
    @PluginProperty
    Boolean warnError = false;

    @Builder.Default
    @Schema(
        title = "Display debug logging during dbt execution.",
        description = "Useful for debugging and making bug reports."
    )
    @PluginProperty
    Boolean debug = false;

    @Schema(
        title = "Which directory to look in for the dbt_project.yml file.",
        description = "Default is the current working directory and its parents."
    )
    @PluginProperty
    String projectDir;

    @Builder.Default
    @Schema(
        title = "The path to the dbt CLI"
    )
    @PluginProperty(dynamic = true)
    String dbtPath = "./bin/dbt";

    @Schema(
        title = "The `profiles.yml` file content",
        description = "If a `profile.yml` file already exist in the current working directory, it will be overridden."
    )
    @PluginProperty(dynamic = true)
    private String profiles;

    // set taskRunner to PROCESS to keep backward compatibility as the old script engine has PROCESS by default and the new DOCKER
    @Schema(
        title = "The task runner to use.",
        description = "Task runners are provided by plugins, each have their own properties."
    )
    @PluginProperty
    @Builder.Default
    @Valid
    protected TaskRunner taskRunner = Process.INSTANCE;

    @Schema(title = "The task runner container image, only used if the task runner is container-based.")
    @PluginProperty(dynamic = true)
    @Builder.Default
    protected String containerImage = DEFAULT_IMAGE;

    @Schema(
        title = "The runner type.",
        description = "Deprecated, use 'taskRunner' instead."
    )
    @Deprecated
    @PluginProperty
    protected RunnerType runner;

    @Schema(
        title = "Deprecated, use 'taskRunner' instead"
    )
    @PluginProperty
    @Deprecated
    private DockerOptions docker;

    @Schema(title = "Deprecated, use the `docker` property instead", deprecated = true)
    @PluginProperty
    @Deprecated
    private DockerOptions dockerOptions;

    @JsonSetter
    public void setDockerOptions(DockerOptions dockerOptions) {
        this.dockerOptions = dockerOptions;
        this.docker = dockerOptions;
    }

    @Schema(
        title = "Additional environment variables for the current process."
    )
    @PluginProperty(
        additionalProperties = String.class,
        dynamic = true
    )
    protected Map<String, String> env;

    @Builder.Default
    @Schema(
        title = "Parse run result",
        description = "Parsing run result to display duration of each task inside dbt"
    )
    @PluginProperty
    protected Boolean parseRunResults = true;

    private NamespaceFiles namespaceFiles;

    private Object inputFiles;

    private List<String> outputFiles;

    protected abstract java.util.List<String> dbtCommands(RunContext runContext, Path workingDirectory) throws IllegalVariableEvaluationException;

    @Override
    public ScriptOutput run(RunContext runContext) throws Exception {
        CommandsWrapper commandsWrapper = new CommandsWrapper(runContext)
            .withEnv(this.getEnv())
            .withNamespaceFiles(namespaceFiles)
            .withInputFiles(inputFiles)
            .withOutputFiles(outputFiles)
            .withRunnerType(this.getRunner())
            .withDockerOptions(this.getDocker())
            .withContainerImage(this.containerImage)
            .withTaskRunner(this.taskRunner)
            .withLogConsumer(new AbstractLogConsumer() {
                @Override
                public void accept(String line, Boolean isStdErr) {
                    LogService.parse(runContext, line);
                }
            });
        Path workingDirectory = commandsWrapper.getWorkingDirectory();

        if (profiles != null && !profiles.isEmpty()) {
            if (Files.exists(Path.of(".profiles/profiles.yml"))) {
                runContext.logger().warn("A 'profiles.yml' file already exist in the task working directory, it will be overridden.");
            }

            FileUtils.writeStringToFile(
                new File(workingDirectory.resolve(".profile").toString(), "profiles.yml"),
                runContext.render(profiles),
                StandardCharsets.UTF_8
            );
        }

        List<String> commandsArgs = ScriptService.scriptCommands(
            List.of("/bin/sh", "-c"),
            null,
            List.of(createDbtCommand(runContext, workingDirectory))
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

    private String createDbtCommand(RunContext runContext, Path workingDirectory) throws IllegalVariableEvaluationException {
        List<String> commands = new ArrayList<>(List.of(
            runContext.render(dbtPath),
            "--log-format json"
        ));

        if (this.debug) {
            commands.add("--debug");
        }

        if (this.failFast) {
            commands.add("--fail-fast");
        }

        if (this.warnError) {
            commands.add("--warn-error");
        }

        commands.addAll(dbtCommands(runContext, workingDirectory));

        if (this.projectDir != null) {
            commands.add("--project-dir " + runContext.render(this.projectDir));
        }

        return String.join(" ", commands);
    }

    protected void parseResults(RunContext runContext, Path workingDirectory, ScriptOutput scriptOutput) throws IllegalVariableEvaluationException, IOException {
        String baseDir = this.projectDir != null ? runContext.render(this.projectDir) : "";

        File runResults = workingDirectory.resolve(baseDir + "target/run_results.json").toFile();

        if (this.parseRunResults && runResults.exists()) {
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
