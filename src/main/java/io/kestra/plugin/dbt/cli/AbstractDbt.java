package io.kestra.plugin.dbt.cli;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.kestra.core.tasks.scripts.BashService;
import io.kestra.plugin.dbt.ResultParser;
import io.kestra.plugin.scripts.exec.scripts.models.DockerOptions;
import io.kestra.plugin.scripts.exec.scripts.models.RunnerType;
import io.kestra.plugin.scripts.exec.scripts.models.ScriptOutput;
import io.kestra.plugin.scripts.exec.scripts.runners.AbstractLogConsumer;
import io.kestra.plugin.scripts.exec.scripts.runners.CommandsWrapper;
import io.kestra.plugin.scripts.exec.scripts.services.ScriptService;
import io.swagger.v3.oas.annotations.media.Schema;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractDbt extends Task implements RunnableTask<ScriptOutput> {
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
        title = "Input files are extra files that will be available in the dbt working directory.",
        description = "You can define the files as map or a JSON string. " +
            "Each file can be defined inlined or can reference a file from Kestra's internal storage."
    )
    @PluginProperty(
        additionalProperties = String.class,
        dynamic = true
    )
    private Object inputFiles;

    @Schema(
        title = "The `profiles.yml` file content",
        description = "If a `profile.yml` file already exist in the current working directory, setting this property will generate an error."
    )
    @PluginProperty(dynamic = true)
    private String profiles;

    // set RunnerType to PROCESS to keep backward compatibility as the old script engine has PROCESS by default and the new DOCKER
    @Builder.Default
    @Schema(
        title = "Runner to use"
    )
    @PluginProperty
    @NotNull
    @NotEmpty
    protected RunnerType runner = RunnerType.PROCESS;

    @Schema(
        title = "Docker options for the `DOCKER` runner"
    )
    @PluginProperty
    @Builder.Default
    private DockerOptions docker = DockerOptions.builder()
        .image("ghcr.io/kestra-io/dbt")
        .entryPoint(List.of())
        .build();

    @Schema(title = "Deprecated, use the `docker` property instead", deprecated = true)
    @PluginProperty
    @Deprecated
    public DockerOptions getDockerOptions() {
        return docker;
    }

    @Deprecated
    public void setDockerOptions(DockerOptions dockerOptions) {
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

    protected abstract java.util.List<String> dbtCommands(RunContext runContext, Path workingDirectory) throws IllegalVariableEvaluationException;

    @Override
    public ScriptOutput run(RunContext runContext) throws Exception {
        CommandsWrapper commandsWrapper = new CommandsWrapper(runContext)
            .withEnv(this.getEnv())
            .withRunnerType(this.getRunner())
            .withDockerOptions(this.getDocker())
            .withLogConsumer(new AbstractLogConsumer() {
                @Override
                public void accept(String line, Boolean isStdErr) throws Exception {
                    LogService.parse(runContext, line);
                }
            });
        Path workingDirectory = commandsWrapper.getWorkingDirectory();

        if (profiles != null && !profiles.isEmpty()) {
            if (Files.exists(Path.of(".profiles/profiles.yml"))) {
                throw new IllegalArgumentException("Cannot use the profiles property if there is already a 'profiles.yml' file");
            }

            FileUtils.writeStringToFile(
                new File(workingDirectory.resolve(".profile").toString(), "profiles.yml"),
                runContext.render(profiles),
                StandardCharsets.UTF_8
            );
        }

        BashService.createInputFiles(
            runContext,
            workingDirectory,
            this.finalInputFiles(runContext),
            Collections.emptyMap()
        );

        List<String> commandsArgs = ScriptService.scriptCommands(
            List.of("/bin/sh", "-c"),
            null,
            List.of(createDbtCommand(runContext, workingDirectory))
        );

        ScriptOutput run = commandsWrapper
            .addEnv(Map.of("PYTHONUNBUFFERED", "true"))
            .withCommands(commandsArgs)
            .run();

        parseResults(runContext, workingDirectory, run);

        return run;
    }

    private Map<String, String> finalInputFiles(RunContext runContext) throws IOException, IllegalVariableEvaluationException {
        return this.inputFiles != null ? new HashMap<>(BashService.transformInputFiles(runContext, this.inputFiles)) : new HashMap<>();
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

        if (runResults.exists()) {
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
