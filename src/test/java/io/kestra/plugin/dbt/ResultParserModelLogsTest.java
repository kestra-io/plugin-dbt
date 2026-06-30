package io.kestra.plugin.dbt;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.slf4j.event.Level;

import io.kestra.core.runners.DynamicTaskRunLog;
import io.kestra.plugin.dbt.models.RunResult;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * Pure unit test for the per-model log lines built from {@code run_results.json}. Runs without a
 * Micronaut context — the routing of these lines to each model's dynamic taskrun (taskRunId, fixed
 * attempt 0) is core's responsibility and is asserted in {@code ResultParserTest}.
 */
class ResultParserModelLogsTest {
    @Test
    void modelLogs_success_summaryAndMessage() {
        var result = RunResult.Result.builder()
            .status("success")
            .message("CREATE VIEW")
            .uniqueId("model.my_project.stg_orders")
            .executionTime(0.42)
            .build();

        List<DynamicTaskRunLog> logs = ResultParser.modelLogs(result);

        assertThat(logs, hasSize(2));
        assertThat(logs.getFirst().level(), is(Level.INFO));
        assertThat(logs.getFirst().message(), allOf(
            containsString("model.my_project.stg_orders"),
            containsString("success"),
            containsString("0.42s")
        ));
        assertThat(logs.getFirst().message(), not(containsString("failures")));
        assertThat(logs.get(1).level(), is(Level.INFO));
        assertThat(logs.get(1).message(), is("CREATE VIEW"));
    }

    @Test
    void modelLogs_error_isErrorLevelWithFailureCount() {
        var result = RunResult.Result.builder()
            .status("error")
            .message("Database Error in model fct_orders\n  relation \"raw_orders\" does not exist")
            .failures(1)
            .uniqueId("model.my_project.fct_orders")
            .executionTime(0.13)
            .build();

        List<DynamicTaskRunLog> logs = ResultParser.modelLogs(result);

        assertThat(logs, hasSize(2));
        assertThat(logs.stream().allMatch(l -> l.level() == Level.ERROR), is(true));
        assertThat(logs.getFirst().message(), allOf(
            containsString("model.my_project.fct_orders"),
            containsString("error"),
            containsString("1 failure")
        ));
        // singular: exactly "1 failure", not "1 failures"
        assertThat(logs.getFirst().message(), not(containsString("failures")));
        assertThat(logs.get(1).message(), containsString("Database Error"));
    }

    @Test
    void modelLogs_multipleFailures_pluralizes() {
        var result = RunResult.Result.builder()
            .status("fail")
            .failures(3)
            .uniqueId("test.my_project.accepted_values_status")
            .build();

        List<DynamicTaskRunLog> logs = ResultParser.modelLogs(result);

        assertThat(logs.getFirst().level(), is(Level.ERROR));
        assertThat(logs.getFirst().message(), containsString("3 failures"));
    }

    @Test
    void modelLogs_warning_isWarnLevelSummaryOnlyWhenNoMessage() {
        var result = RunResult.Result.builder()
            .status("warn")
            .uniqueId("model.my_project.snapshot")
            .build();

        List<DynamicTaskRunLog> logs = ResultParser.modelLogs(result);

        // no message, no execution time, no failures => only the summary line
        assertThat(logs, hasSize(1));
        assertThat(logs.getFirst().level(), is(Level.WARN));
        assertThat(logs.getFirst().message(), allOf(
            containsString("model.my_project.snapshot"),
            containsString("warn")
        ));
        assertThat(logs.getFirst().message(), not(containsString("failures")));
    }

    @Test
    void modelLogs_zeroFailures_doesNotMentionFailures() {
        var result = RunResult.Result.builder()
            .status("pass")
            .failures(0)
            .uniqueId("test.my_project.not_null_orders")
            .build();

        List<DynamicTaskRunLog> logs = ResultParser.modelLogs(result);

        assertThat(logs, hasSize(1));
        assertThat(logs.getFirst().level(), is(Level.INFO));
        assertThat(logs.getFirst().message(), not(containsString("failures")));
    }
}
