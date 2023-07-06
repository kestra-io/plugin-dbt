package io.kestra.plugin.dbt.cli;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.utils.ListUtils;
import io.kestra.plugin.dbt.ResultParser;
import io.kestra.plugin.scripts.exec.AbstractExecScript;
import io.kestra.plugin.scripts.exec.scripts.models.DockerOptions;
import io.kestra.plugin.scripts.exec.scripts.models.ScriptOutput;
import io.kestra.plugin.scripts.exec.scripts.runners.AbstractLogConsumer;
import io.kestra.plugin.scripts.exec.scripts.runners.CommandsWrapper;
import io.kestra.plugin.scripts.exec.scripts.services.ScriptService;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Execute dbt commands."
)
public class Commands extends AbstractExecScript {
    static final private ObjectMapper MAPPER = JacksonMapper.ofYaml();

    @Schema(
        title = "The commands to run"
    )
    @PluginProperty(dynamic = true)
    @NotNull
    @NotEmpty
    protected List<String> commands;


    @Schema(
        title = "The profiles file content"
    )
    @PluginProperty(dynamic = true)
    @NotNull
    @NotEmpty
    Map<String, Object> profiles;

    @Override
    protected DockerOptions defaultDockerOptions() {
        return DockerOptions.builder()
            .image("dbt-bigquery")
            .entryPoint(List.of())
            .build();
    }

    @Override
    public ScriptOutput run(RunContext runContext) throws Exception {
        CommandsWrapper commands = this.commands(runContext)
            .withLogConsumer(new AbstractLogConsumer() {
                @Override
                public void accept(String line, Boolean isStdErr) throws Exception {
                    LogService.parse(runContext, line);
                }
            });

        // write profile
        File profileDir = commands.getWorkingDirectory().resolve(".profile").toFile();

        // noinspection ResultOfMethodCallIgnored
        profileDir.mkdirs();

        FileUtils.writeStringToFile(
            new File(profileDir, "profiles.yml"),
            MAPPER.writeValueAsString(runContext.render(profiles)),
            StandardCharsets.UTF_8
        );

        List<String> commandsArgs = ScriptService.scriptCommands(
            this.interpreter,
            this.beforeCommands,
            this.commands
        );

        ScriptOutput run = this.commands(runContext)
            .addAdditionalVars(Map.of("profileDir", profileDir.getAbsolutePath()))
            .addEnv(Map.of("PYTHONUNBUFFERED", "true"))
            .withCommands(commandsArgs)
            .run();

        if (commands.getWorkingDirectory().resolve("target/run_results.json").toFile().exists()) {
            ResultParser.parseRunResult(runContext, commands.getWorkingDirectory().resolve("target/run_results.json").toFile());
        }

        if (commands.getWorkingDirectory().resolve("target/manifest.json").toFile().exists()) {
            ResultParser.parseManifest(runContext, commands.getWorkingDirectory().resolve("target/manifest.json").toFile());
        }

        return run;
    }
}
