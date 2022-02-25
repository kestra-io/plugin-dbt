package io.kestra.plugin.dbt.cli;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.tasks.scripts.AbstractPython;
import io.kestra.core.tasks.scripts.RunResult;
import io.kestra.core.tasks.scripts.ScriptOutput;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Setup dbt",
    description = "Install pip package & init profile directory"
)
public class Setup extends AbstractPython implements RunnableTask<ScriptOutput> {
    static final private ObjectMapper MAPPER = JacksonMapper.ofYaml();

    @Schema(
        title = "The profiles file content"
    )
    @PluginProperty(dynamic = true)
    @NotNull
    @NotEmpty
    Map<String, Object> profiles;

    @Override
    public ScriptOutput run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();

        if (this.workingDirectory == null) {
            this.workingDirectory = runContext.tempDir();
        }

        String command = this.virtualEnvCommand(runContext, runContext.render(this.getRequirements()));

        RunResult runResult = this.run(
            runContext,
            logger,
            workingDirectory,
            finalCommandsWithInterpreter(command),
            this.finalEnv(),
            this.defaultLogSupplier(logger, runContext)
        );

        // write profile
        File profileDir = workingDirectory.resolve(".profile").toFile();
        // noinspection ResultOfMethodCallIgnored
        profileDir.mkdirs();

        FileUtils.writeStringToFile(
            new File(profileDir, "profiles.yml"),
            MAPPER.writeValueAsString(runContext.render(profiles)),
            StandardCharsets.UTF_8
        );

        // output
        return ScriptOutput.builder()
            .exitCode(runResult.getExitCode())
            .stdOutLineCount(runResult.getStdOut().getLogsCount())
            .stdErrLineCount(runResult.getStdErr().getLogsCount())
            .warningOnStdErr(this.warningOnStdErr)
            .build();
    }
}
