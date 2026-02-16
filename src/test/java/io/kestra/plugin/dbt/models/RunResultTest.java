package io.kestra.plugin.dbt.models;

import io.kestra.core.models.flows.State;
import org.junit.jupiter.api.Test;

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
}
