package io.kestra.plugin.dbt.models;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.kestra.core.models.flows.State;

import lombok.Value;
import lombok.experimental.SuperBuilder;
import lombok.extern.jackson.Jacksonized;

@Value
@Jacksonized
@SuperBuilder
@JsonIgnoreProperties(ignoreUnknown = true)
public class RunResult {
    List<Result> results;

    @JsonProperty("elapsed_time")
    Double elapsedTime;

    Map<String, Object> args;

    @Value
    @Jacksonized
    @SuperBuilder
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Result {
        String status;

        List<Timing> timing;

        @JsonProperty("thread_id")
        String threadId;

        @JsonProperty("execution_time")
        Double executionTime;

        @JsonProperty("adapter_response")
        Map<String, String> adapterResponse;

        String message;

        Integer failures;

        @JsonProperty("unique_id")
        String uniqueId;

        public State.Type state() {
            if (this.status == null)
                return State.Type.FAILED;
            return switch (this.status) {
                // dbt writes "runtime error" with a space (NodeStatus.RuntimeErr)
                case "error", "fail", "runtime error", "runtime_error" -> State.Type.FAILED;
                case "warn" -> State.Type.WARNING;
                case "skipped" -> State.Type.SKIPPED;
                // Fusion v2.0 emits "run" for an executed model, and dbt build "no-op" for nodes with nothing to run, such as exposures
                case "success", "pass", "run", "no-op" -> State.Type.SUCCESS;
                default -> throw new IllegalStateException("No suitable status for '" + this.status + "'");
            };
        }
    }

    @Value
    @Jacksonized
    @SuperBuilder
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Timing {
        String name;

        @JsonProperty("started_at")
        Instant startedAt;

        @JsonProperty("completed_at")
        Instant completedAt;
    }
}
