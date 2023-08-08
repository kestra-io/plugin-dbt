package io.kestra.plugin.dbt.cli;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.tasks.scripts.AbstractBash;
import io.kestra.core.tasks.scripts.AbstractLogThread;
import io.kestra.core.tasks.scripts.ScriptOutput;
import io.kestra.plugin.dbt.ResultParser;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.ArrayList;

import static io.kestra.core.utils.Rethrow.throwSupplier;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractDbt extends AbstractBash implements RunnableTask<ScriptOutput> {
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

    protected abstract java.util.List<String> commands(RunContext runContext) throws IllegalVariableEvaluationException;

    @Override
    public ScriptOutput run(RunContext runContext) throws Exception {
        if (this.workingDirectory == null) {
            this.workingDirectory = runContext.tempDir();
        }

        ScriptOutput run = run(runContext, throwSupplier(() -> {
            java.util.List<String> commands = new ArrayList<>(java.util.List.of(
                runContext.render(dbtPath),
                "--log-format json"
            ));

            if (this.projectDir != null) {
                commands.add("--project-dir " + runContext.render(this.projectDir));
            }

            if (this.debug) {
                commands.add("--debug");
            }


            if (this.failFast) {
                commands.add("--fail-fast");
            }

            if (this.warnError) {
                commands.add("--warn-error");
            }

            commands.addAll(commands(runContext));

            return String.join(" ", commands);
        }));

        parseResults(runContext);

        return run;
    }

    protected void parseResults(RunContext runContext) throws IllegalVariableEvaluationException, IOException {
        ResultParser.parseRunResult(runContext, this.workingDirectory.resolve("target/run_results.json").toFile());
        ResultParser.parseManifest(runContext, this.workingDirectory.resolve("target/manifest.json").toFile());
    }

    @Override
    protected LogSupplier defaultLogSupplier(Logger logger, RunContext runContext) {
        return (inputStream, isStdErr) -> {
            AbstractLogThread thread;
            thread = new DbtLogParser(inputStream, logger, runContext);
            thread.setName("dbt-log-" + (isStdErr ? "-err" : "-out"));

            thread.start();

            return thread;
        };
    }
}
