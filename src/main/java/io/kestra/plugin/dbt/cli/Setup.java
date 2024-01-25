package io.kestra.plugin.dbt.cli;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.tasks.PluginUtilsService;
import io.kestra.plugin.scripts.exec.AbstractExecScript;
import io.kestra.plugin.scripts.exec.scripts.models.DockerOptions;
import io.kestra.plugin.scripts.exec.scripts.models.RunnerType;
import io.kestra.plugin.scripts.exec.scripts.models.ScriptOutput;
import io.kestra.plugin.scripts.exec.scripts.runners.CommandsWrapper;
import io.kestra.plugin.scripts.exec.scripts.services.ScriptService;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
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
@Schema(
    title = "Setup dbt in a Python virtualenv",
    description = """
        Use it to install dbt requirements locally in a Python virtualenv if you don't want to use dbt via Docker.
        In this case, you need to use a `WorkingDirectory` task and this `Setup` task to setup dbt prior to using any of the dbt tasks."""
)
@Plugin(
    examples = {
        @Example(
            full = true,
            title = "Setup dbt by installing pip dependencies in a Python virtualenv and initializing the profile directory",
            code = """
                namespace: io.kestra.tests
                id: dbt-setup
                tasks:
                  - id: working-directory
                    type: io.kestra.core.tasks.flows.WorkingDirectory
                    tasks:
                    - id: cloneRepository
                      type: io.kestra.plugin.git.Clone
                      url: https://github.com/kestra-io/dbt-demo
                      branch: main
                    - id: dbt-setup
                      type: io.kestra.plugin.dbt.cli.Setup
                      requirements:
                        - dbt-duckdb
                      profiles:
                        jaffle_shop:
                          outputs:
                            dev:
                              type: duckdb
                              path: ':memory:'
                              extensions:
                                - parquet
                          target: dev
                    - id: dbt-build
                      type: io.kestra.plugin.dbt.cli.Build
                """
        )
    }
)
public class Setup extends AbstractExecScript implements RunnableTask<ScriptOutput> {
    static final private ObjectMapper MAPPER = JacksonMapper.ofYaml();

    @Schema(
        title = "The `profiles.yml` file content. Can be an object (a map) or a string.",
        anyOf = { Map.class, String.class }
    )
    @PluginProperty(dynamic = true)
    @NotNull
    @NotEmpty
    Object profiles;

    @Builder.Default
    @Schema(
        title = "The python interpreter to use",
        description = "Set the python interpreter path to use"
    )
    @PluginProperty(dynamic = true)
    @NotNull
    @NotEmpty
    private final String pythonPath = "python";

    @Schema(
        title = "List of python dependencies to add to the python execution process",
        description = "Python dependencies list to setup in the virtualenv, in the same format than requirements.txt. It must at least provides dbt."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    protected List<String> requirements;

    @Builder.Default
    @Schema(
        title = "Exit if any non true return value",
        description = "This tells bash that it should exit the script if any statement returns a non-true return value. \n" +
            "The benefit of using -e is that it prevents errors snowballing into serious issues when they could " +
            "have been caught earlier."
    )
    @PluginProperty
    @NotNull
    protected Boolean exitOnFailed = true;

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
    protected DockerOptions docker = DockerOptions.builder()
        .image("python")
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

    @Override
    public ScriptOutput run(RunContext runContext) throws Exception {
        CommandsWrapper commandsWrapper = this.commands(runContext);
        Path workingDirectory = commandsWrapper.getWorkingDirectory();

        List<String> commands = this.virtualEnvCommand(runContext, workingDirectory, this.getRequirements() != null ? runContext.render(this.getRequirements()) : null);

        // write profile
        File profileDir = workingDirectory.resolve(".profile").toFile();
        // noinspection ResultOfMethodCallIgnored
        profileDir.mkdirs();

        String profilesContent = profilesContent(runContext, profiles);
        FileUtils.writeStringToFile(
            new File(profileDir, "profiles.yml"),
            profilesContent,
            StandardCharsets.UTF_8
        );

        PluginUtilsService.createInputFiles(
            runContext,
            workingDirectory,
            this.finalInputFiles(runContext),
            Collections.emptyMap()
        );

        List<String> commandsArgs = ScriptService.scriptCommands(
            this.interpreter,
            this.beforeCommands,
            commands
        );

        return commandsWrapper
            .addEnv(Map.of(
                "PYTHONUNBUFFERED", "true",
                "PIP_ROOT_USER_ACTION", "ignore"
            ))
            .withCommands(commandsArgs)
            .run();
    }

    private List<String> virtualEnvCommand(RunContext runContext, Path workingDirectory, List<String> requirements) throws IllegalVariableEvaluationException {
        List<String> renderer = new ArrayList<>();

        if (this.exitOnFailed) {
            renderer.add("set -o errexit");
        }
        renderer.add(this.pythonPath + " -m venv --system-site-packages " + workingDirectory + " > /dev/null");

        if (requirements != null) {
            renderer.addAll(Arrays.asList(
                "./bin/pip install pip --upgrade > /dev/null",
                "./bin/pip install " + runContext.render(String.join(" ", requirements) + " > /dev/null")));
        }

        return renderer;
    }

    private String profilesContent(RunContext runContext, Object profiles) throws IllegalVariableEvaluationException, JsonProcessingException {
        if (profiles instanceof String content) {
            return content;
        }
        if(profiles instanceof Map contentMap) {
            return MAPPER.writeValueAsString(runContext.render(contentMap));
        }
        throw new IllegalArgumentException("The `profiles` attribute must be a String or a Map");
    }

    private Map<String, String> finalInputFiles(RunContext runContext) throws IOException, IllegalVariableEvaluationException {
        return this.inputFiles != null ? new HashMap<>(PluginUtilsService.transformInputFiles(runContext, this.inputFiles)) : new HashMap<>();
    }
}
