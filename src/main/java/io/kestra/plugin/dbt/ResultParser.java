package io.kestra.plugin.dbt;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.executions.TaskRun;
import io.kestra.core.models.executions.TaskRunAttempt;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.flows.State;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.WorkerTaskResult;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.utils.IdUtils;
import io.kestra.plugin.dbt.models.RunResult;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static io.kestra.core.utils.Rethrow.throwFunction;

public abstract class ResultParser {
    static final protected ObjectMapper MAPPER = JacksonMapper.ofJson(false)
        .setSerializationInclusion(JsonInclude.Include.NON_NULL);

    public static URI parseManifest(RunContext runContext, File file) throws IOException {
        return runContext.storage().putFile(file);
    }

    public static URI parseRunResult(RunContext runContext, File file) throws IOException, IllegalVariableEvaluationException {
        RunResult result = MAPPER.readValue(
            file,
            RunResult.class
        );

        java.util.List<WorkerTaskResult> workerTaskResults = result
            .getResults()
            .stream()
            .map(throwFunction(r -> {
                ArrayList<State.History> histories = new ArrayList<>();

                // List of status are not safe and can be not present on api calls
                r.getTiming()
                    .stream()
                    .mapToLong(timing -> timing.getStartedAt().toEpochMilli())
                    .min()
                    .ifPresent(value -> {
                        histories.add(new State.History(
                            State.Type.CREATED,
                            Instant.ofEpochMilli(value)
                        ));
                    });

                r.getTiming()
                    .stream()
                    .filter(timing -> timing.getName().equals("execute"))
                    .mapToLong(timing -> timing.getStartedAt().toEpochMilli())
                    .min()
                    .ifPresent(value -> {
                        histories.add(new State.History(
                            State.Type.RUNNING,
                            Instant.ofEpochMilli(value)
                        ));
                    });

                r.getTiming()
                    .stream()
                    .mapToLong(timing -> timing.getStartedAt().toEpochMilli())
                    .max()
                    .ifPresent(value -> {
                        histories.add(new State.History(
                            r.state(),
                            Instant.ofEpochMilli(value)
                        ));
                    });

                State state = State.of(
                    r.state(),
                    histories
                );

                r.getAdapterResponse()
                    .entrySet()
                    .stream()
                    .map(e -> {
                        return switch (e.getKey()) {
                            case "rows_affected" -> Counter.of("rows.affected", Double.valueOf(e.getValue()));
                            case "bytes_processed" -> Counter.of("bytes.processed", Double.valueOf(e.getValue()));
                            default -> null;
                        };
                    })
                    .filter(Objects::nonNull)
                    .forEach(runContext::metric);

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
                        .attempts(List.of(TaskRunAttempt.builder()
                            .state(state)
                            .build()
                        ))
                        .build()
                    )
                    .build();
            }))
            .toList();

        runContext.dynamicWorkerResult(workerTaskResults);

        return runContext.storage().putFile(file);
    }
}
