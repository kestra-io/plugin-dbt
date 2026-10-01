package io.kestra.plugin.dbt.models;

import java.util.Comparator;
import java.util.List;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;

@Value
@Builder
@Jacksonized
@JsonIgnoreProperties(ignoreUnknown = true)
public class RunSummary {
    private static final int SLOWEST_LIMIT = 3;

    @Schema(title = "Number of executed non-test nodes (models, seeds, snapshots, ...)")
    int total;

    @Schema(title = "Nodes that succeeded")
    int success;

    @Schema(title = "Nodes that errored")
    int error;

    @Schema(title = "Nodes that finished with a warning")
    int warn;

    @Schema(title = "Nodes that were skipped")
    int skipped;

    @Schema(title = "Total dbt run duration, in seconds, as reported by dbt")
    Double elapsedTime;

    @Schema(title = "Up to 3 slowest nodes, slowest first")
    List<SlowNode> slowest;

    @Value
    @Builder
    @Jacksonized
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class SlowNode {
        @Schema(title = "dbt unique_id of the node, e.g. `model.shop.orders`")
        String uniqueId;

        @Schema(title = "Execution time in seconds")
        Double executionTime;
    }

    public static RunSummary from(RunResult runResult) {
        List<RunResult.Result> nodes = results(runResult).stream()
            .filter(r -> !TestSummary.isTest(r) && !isHook(r))
            .toList();

        int success = 0, error = 0, warn = 0, skipped = 0;
        for (RunResult.Result r : nodes) {
            switch (Objects.requireNonNullElse(r.getStatus(), "")) {
                // Fusion v2.0 emits "run" for a successfully executed model, "pass" matches RunResult.Result#state
                case "success", "run", "pass" -> success++;
                case "error", "runtime_error", "fail" -> error++;
                // microbatch models report "partial success" when some batches failed
                case "warn", "partial success" -> warn++;
                case "skipped", "no-op" -> skipped++;
                default -> {
                }
            }
        }

        List<SlowNode> slowest = nodes.stream()
            .filter(r -> r.getExecutionTime() != null)
            .sorted(Comparator.comparing(RunResult.Result::getExecutionTime).reversed())
            .limit(SLOWEST_LIMIT)
            .map(r -> SlowNode.builder().uniqueId(r.getUniqueId()).executionTime(r.getExecutionTime()).build())
            .toList();

        return RunSummary.builder()
            .total(nodes.size())
            .success(success)
            .error(error)
            .warn(warn)
            .skipped(skipped)
            .elapsedTime(runResult == null ? null : runResult.getElapsedTime())
            .slowest(slowest)
            .build();
    }

    // on-run-start/on-run-end hooks are reported as operation.* results, they are not project nodes.
    private static boolean isHook(RunResult.Result result) {
        return result.getUniqueId() != null && result.getUniqueId().startsWith("operation.");
    }

    static List<RunResult.Result> results(RunResult runResult) {
        return runResult == null || runResult.getResults() == null ? List.of() : runResult.getResults();
    }
}
