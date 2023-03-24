package io.kestra.plugin.dbt;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.executions.AbstractMetricEntry;
import io.kestra.core.models.executions.TaskRun;
import io.kestra.core.models.executions.TaskRunAttempt;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.flows.State;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.WorkerTaskResult;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.utils.IdUtils;
import io.kestra.plugin.dbt.models.Manifest;
import io.kestra.plugin.dbt.models.RunResult;

import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.util.Objects;
import java.util.stream.Collectors;

import static io.kestra.core.utils.Rethrow.throwFunction;

public abstract class ResultParser {
    static final protected ObjectMapper MAPPER = JacksonMapper.ofJson(false)
        .setSerializationInclusion(JsonInclude.Include.NON_NULL);

    public static Manifest parseManifest(RunContext runContext, File file) throws IOException {
        return MAPPER.readValue(
            file,
            Manifest.class
        );
    }

    public static RunResult parseRunResult(RunContext runContext, File file) throws IOException, IllegalVariableEvaluationException {
        RunResult result = MAPPER.readValue(
            file,
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
                            .build()
                        ))
                        .build()
                    )
                    .build();
            }))
            .collect(Collectors.toList());

        runContext.dynamicWorkerResult(workerTaskResults);

        return result;
    }
}
