package io.kestra.plugin.dbt.cli;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.AbstractMetricEntry;
import io.kestra.core.models.executions.TaskRun;
import io.kestra.core.models.executions.TaskRunAttempt;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.flows.State;
import io.kestra.core.models.tasks.DynamicTask;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.WorkerTaskResult;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.tasks.scripts.AbstractBash;
import io.kestra.core.tasks.scripts.AbstractLogThread;
import io.kestra.core.tasks.scripts.ScriptOutput;
import io.kestra.core.utils.IdUtils;
import io.kestra.plugin.dbt.cli.models.Manifest;
import io.kestra.plugin.dbt.cli.models.RunResult;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;

import java.io.IOException;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

import static io.kestra.core.utils.Rethrow.throwFunction;
import static io.kestra.core.utils.Rethrow.throwSupplier;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractDbt extends AbstractBash implements RunnableTask<ScriptOutput>, DynamicTask {
    static final protected ObjectMapper MAPPER = JacksonMapper.ofJson(false)
        .setSerializationInclusion(JsonInclude.Include.NON_NULL);

    @Builder.Default
    @Schema(
        title = "Stop execution upon a first failure."
    )
    @PluginProperty(dynamic = false)
    Boolean failFast = false;

    @Builder.Default
    @Schema(
        title = "If dbt would normally warn, instead raise an exception.",
        description = "Examples include --models that selects nothing, deprecations, configurations with no " +
            "associated models, invalid test configurations, and missing sources/refs in tests."
    )
    @PluginProperty(dynamic = false)
    Boolean warnError = false;

    @Builder.Default
    @Schema(
        title = "Display debug logging during dbt execution.",
        description = "Useful for debugging and making bug reports."
    )
    @PluginProperty(dynamic = false)
    Boolean debug = false;

    @Builder.Default
    @Schema(
        title = "The path to dbt cli"
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
                "--log-format=json",
                "--profiles-dir=" + this.workingDirectory.resolve(".profile").toAbsolutePath()
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

            commands.addAll(commands(runContext));

            return String.join(" ", commands);
        }));

        parseRunResult(runContext);

        return run;
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

    protected Manifest parseManifest() throws IOException {
        return MAPPER.readValue(
            this.workingDirectory.resolve("target/manifest.json").toFile(),
            Manifest.class
        );
    }

    protected void parseRunResult(RunContext runContext) throws IOException, IllegalVariableEvaluationException {
        RunResult result = MAPPER.readValue(
            this.workingDirectory.resolve("target/run_results.json").toFile(),
            RunResult.class
        );

        java.util.List<WorkerTaskResult> workerTaskResults = result
            .getResults()
            .stream()
            .map(throwFunction(r -> {
                State state = State.of(
                    r.state(),
                    java.util.List.of(
                        new State.History(
                            State.Type.CREATED,
                            Instant.ofEpochMilli(r.getTiming()
                                .stream()
                                .mapToLong(timing -> timing.getStartedAt().toEpochMilli())
                                .min()
                                .orElseThrow())
                        ),
                        new State.History(
                            State.Type.RUNNING,
                            Instant.ofEpochMilli(r.getTiming()
                                .stream()
                                .filter(timing -> timing.getName().equals("execute"))
                                .mapToLong(timing -> timing.getStartedAt().toEpochMilli())
                                .min()
                                .orElseThrow())
                        ),
                        new State.History(
                            r.state(),
                            Instant.ofEpochMilli(r.getTiming()
                                .stream()
                                .mapToLong(timing -> timing.getCompletedAt().toEpochMilli())
                                .max()
                                .orElseThrow())
                        )
                    )
                );

                java.util.List<AbstractMetricEntry<?>> metrics = r.getAdapterResponse()
                    .entrySet()
                    .stream()
                    .map(e -> {
                        switch (e.getKey()) {
                            case "rows_affected":
                                return Counter.of("rows.affected", Double.valueOf(e.getValue()));
                            case "bytes_processed":
                                return Counter.of("bytes.processed", Double.valueOf(e.getValue()));
                        }

                        return null;
                    })
                    .filter(Objects::nonNull)
                    .peek(runContext::metric)
                    .collect(Collectors.toList());

                return WorkerTaskResult.builder()
                    .taskRun(TaskRun.builder()
                        .id(IdUtils.create())
                        .namespace(runContext.render("{{ flow.namespace }}"))
                        .flowId(runContext.render("{{ flow.id }}"))
                        .taskId(r.getUniqueId())
                        .value(runContext.render("{{ taskrun.id }}"))
                        .executionId(runContext.render("{{ execution.id }}"))
                        .parentTaskRunId(runContext.render("{{ taskrun.id }}"))
                        .state(state)
                        .attempts(java.util.List.of(TaskRunAttempt.builder()
                            .state(state)
                            .metrics(metrics)
                            .build()
                        ))
                        .build()
                    )
                    .build();
            }))
            .collect(Collectors.toList());

        runContext.dynamicWorkerResult(workerTaskResults);
    }
}
