package io.kestra.plugin.dbt.models;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import io.kestra.core.models.flows.State;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

class RunResultTest {
    @Test
    void state_withSkippedStatus_shouldReturnSkipped() {
        var result = RunResult.Result.builder()
            .status("skipped")
            .build();

        assertThat(result.state(), is(State.Type.SKIPPED));
    }

    @ParameterizedTest
    @CsvSource({
        "success, SUCCESS",
        "pass, SUCCESS",
        // Fusion v2.0 emits "run" for a successfully executed model
        "run, SUCCESS",
        "error, FAILED",
        "fail, FAILED",
        "runtime_error, FAILED",
        "warn, WARNING",
        "skipped, SKIPPED"
    })
    void state_allStatuses(String status, String expectedState) {
        var result = RunResult.Result.builder()
            .status(status)
            .build();

        assertThat(result.state(), is(State.Type.valueOf(expectedState)));
    }
}
