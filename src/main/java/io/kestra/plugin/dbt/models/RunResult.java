package io.kestra.plugin.dbt.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.kestra.core.models.flows.State;
import lombok.Value;
import lombok.experimental.SuperBuilder;
import lombok.extern.jackson.Jacksonized;

import java.time.Instant;
import java.util.List;
import java.util.Map;

@Value
@Jacksonized
@SuperBuilder
public class RunResult {
    List<Result> results;

    @JsonProperty("elapsed_time")
    Double elapsedTime;

    Map<String, Object> args;

    @Value
    @Jacksonized
    @SuperBuilder
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
            switch (this.status) {
                case "error":
                case "fail":
                case "runtime_error":
                    return State.Type.FAILED;
                case "warn":
                    return State.Type.WARNING;
                case "skipped":
                    return State.Type.SKIPPED;
                case "success":
                case "pass":
                    return State.Type.SUCCESS;
            }

            throw new IllegalStateException("No suitable status for '" + this.status + "'");
        }
    }

    @Value
    @Jacksonized
    @SuperBuilder
    public static class Timing {
        String name;

        @JsonProperty("started_at")
        Instant startedAt;

        @JsonProperty("completed_at")
        Instant completedAt;
    }
}
