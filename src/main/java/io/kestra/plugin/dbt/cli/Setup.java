package io.kestra.plugin.dbt.cli;

import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.runners.PluginUtilsService;
import io.kestra.core.models.tasks.runners.ScriptService;
import io.kestra.core.models.tasks.runners.TaskRunner;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.plugin.scripts.exec.AbstractExecScript;
import io.kestra.plugin.scripts.exec.scripts.models.DockerOptions;
import io.kestra.plugin.scripts.exec.scripts.models.ScriptOutput;
import io.kestra.plugin.scripts.exec.scripts.runners.CommandsWrapper;
import io.kestra.plugin.scripts.runner.docker.Docker;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.Valid;
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
import java.util.HashMap;
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
    title = "Setup dbt in a Python virtualenv.",
    description = """
        Use it to install dbt requirements locally in a Python virtualenv if you don't want to use dbt via Docker.
        In this case, you need to use a `WorkingDirectory` task and this `Setup` task to setup dbt prior to using any of the dbt tasks."""
)
@Plugin(
    examples = {
        @Example(
            full = true,
            title = "Setup dbt by installing pip dependencies in a Python virtualenv and initializing the profile directory.",
            code = """
                id: dbt_setup
                namespace: company.team

                tasks:
                  - id: working_directory
                    type: io.kestra.plugin.core.flow.WorkingDirectory
                    tasks:
                      - id: clone_repository
                        type: io.kestra.plugin.git.Clone
                        url: https://github.com/kestra-io/dbt-demo
                        branch: main
                    
                      - id: dbt_setup
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
                    
                      - id: dbt_build
                        type: io.kestra.plugin.dbt.cli.Build
                """
        )
    }
)
public class Setup extends AbstractExecScript implements RunnableTask<ScriptOutput> {
    static final private ObjectMapper MAPPER = JacksonMapper.ofYaml();

    private static final String DEFAULT_IMAGE = "python";

    @Schema(
        title = "The `profiles.yml` file content. Can be an object (a map) or a string.",
        anyOf = { Map.class, String.class }
    )
    @NotNull
    Property<Object> profiles;

    @Builder.Default
    @Schema(
        title = "The python interpreter to use.",
        description = "Set the python interpreter path to use."
    )
    @NotNull
    @NotEmpty
    @PluginProperty(dynamic = true)
    private final String pythonPath = DEFAULT_IMAGE;

    @Schema(
        title = "List of python dependencies to add to the python execution process.",
        description = "Python dependencies list to setup in the virtualenv, in the same format than requirements.txt. It must at least provides dbt."
    )
    @NotNull
    protected Property<List<String>> requirements;

    @Builder.Default
    @Schema(
        title = "Exit if any non true return value.",
        description = "This tells bash that it should exit the script if any statement returns a non-true return value. \n" +
            "The benefit of using -e is that it prevents errors snowballing into serious issues when they could " +
            "have been caught earlier. This option is deprecated. Use `failFast` instead."
    )
    @NotNull
    @Deprecated(since = "0.16.0", forRemoval = true)
    protected Property<Boolean> exitOnFailed = Property.of(Boolean.TRUE);

    @Schema(
        title = "Input files are extra files that will be available in the dbt working directory.",
        description = "You can define the files as map or a JSON string. " +
            "Each file can be defined inlined or can reference a file from Kestra's internal storage."
    )
    private Property<Object> inputFiles;

    @Schema(
        title = "The task runner to use.",
        description = "Task runners are provided by plugins, each have their own properties."
    )
    @Builder.Default
    @PluginProperty
    @Valid
    protected TaskRunner taskRunner = Docker.instance();

    @Builder.Default
    protected String containerImage = DEFAULT_IMAGE;

    @Schema(title = "Deprecated, use the `docker` property instead", deprecated = true)
    @Deprecated
    private Property<DockerOptions> dockerOptions;

    @JsonSetter
    public void setDockerOptions(DockerOptions dockerOptions) {
        this.dockerOptions = Property.of(dockerOptions);
        this.docker = dockerOptions;
    }

    @Override
    protected DockerOptions injectDefaults(DockerOptions original) {
        var builder = original.toBuilder();
        if (original.getImage() == null) {
            builder.image(this.getContainerImage());
        }

        return builder.build();
    }

    @Override
    public ScriptOutput run(RunContext runContext) throws Exception {
        CommandsWrapper commandsWrapper = this.commands(runContext);
        Path workingDirectory = commandsWrapper.getWorkingDirectory();

        List<String> commands = this.virtualEnvCommand(runContext, workingDirectory, this.requirements.asList(runContext, String.class));

        // write profile
        File profileDir = workingDirectory.resolve(".profile").toFile();
        // noinspection ResultOfMethodCallIgnored
        profileDir.mkdirs();

        String profilesContent = profilesContent(runContext, profiles.as(runContext, Object.class));
        FileUtils.writeStringToFile(
            new File(profileDir, "profiles.yml"),
            profilesContent,
            StandardCharsets.UTF_8
        );

        PluginUtilsService.createInputFiles(
            runContext,
            workingDirectory,
            this.finalInputFiles(runContext),
            new HashMap<>()
        );

        List<String> commandsArgs = ScriptService.scriptCommands(
            this.interpreter,
            this.getBeforeCommandsWithOptions(),
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

        renderer.add(runContext.render(this.pythonPath) + " -m venv --system-site-packages " + workingDirectory + " > /dev/null");

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
        return this.inputFiles != null ? new HashMap<>(PluginUtilsService.transformInputFiles(runContext, this.inputFiles.as(runContext, Object.class))) : new HashMap<>();
    }
}
