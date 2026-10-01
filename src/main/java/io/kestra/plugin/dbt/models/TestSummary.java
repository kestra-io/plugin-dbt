package io.kestra.plugin.dbt.models;

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
public class TestSummary {
    @Schema(title = "Number of executed data and unit tests")
    int total;

    @Schema(title = "Tests that passed")
    int pass;

    @Schema(title = "Tests that failed")
    int fail;

    @Schema(title = "Tests that finished with a warning")
    int warn;

    @Schema(title = "Tests that errored before they could evaluate")
    int error;

    @Schema(title = "Tests that were skipped")
    int skipped;

    public static TestSummary from(RunResult runResult) {
        List<RunResult.Result> tests = RunSummary.results(runResult).stream()
            .filter(TestSummary::isTest)
            .toList();

        int pass = 0, fail = 0, warn = 0, error = 0, skipped = 0;
        for (RunResult.Result r : tests) {
            switch (Objects.requireNonNullElse(r.getStatus(), "")) {
                case "pass", "success" -> pass++;
                case "fail" -> fail++;
                case "warn" -> warn++;
                case "error", "runtime_error" -> error++;
                case "skipped" -> skipped++;
                default -> {
                }
            }
        }

        return TestSummary.builder()
            .total(tests.size())
            .pass(pass)
            .fail(fail)
            .warn(warn)
            .error(error)
            .skipped(skipped)
            .build();
    }

    // dbt prefixes unique_id with the resource type, so tests are identifiable without the manifest.
    static boolean isTest(RunResult.Result result) {
        String id = result.getUniqueId();
        return id != null && (id.startsWith("test.") || id.startsWith("unit_test."));
    }
}
