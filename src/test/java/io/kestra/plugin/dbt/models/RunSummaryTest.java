package io.kestra.plugin.dbt.models;

import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;

import io.kestra.core.serializers.JacksonMapper;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

class RunSummaryTest {
    private static RunResult.Result result(String uniqueId, String status, Double executionTime) {
        return RunResult.Result.builder().uniqueId(uniqueId).status(status).executionTime(executionTime).build();
    }

    private static RunResult runResult(RunResult.Result... results) {
        return RunResult.builder().results(Arrays.asList(results)).elapsedTime(12.5).build();
    }

    @Test
    void summarizesNodesAndTestsSeparately() {
        var runResult = runResult(
            result("model.shop.customers", "success", 1.0),
            // Fusion v2.0 emits "run" for a successfully executed model
            result("model.shop.orders", "run", 42.0),
            result("model.shop.payments", "error", 3.0),
            result("seed.shop.countries", "success", 0.5),
            result("snapshot.shop.orders_snapshot", "skipped", null),
            result("operation.shop.shop-on-run-end-0", "success", 99.0),
            result("test.shop.not_null_orders_customer_id.a1b2", "fail", 0.2),
            result("test.shop.unique_orders_id.c3d4", "pass", 0.1),
            result("test.shop.accepted_values_status.e5f6", "warn", 0.1),
            result("test.shop.relationships_orders.g7h8", "skipped", null),
            result("unit_test.shop.orders.test_totals", "pass", 0.3)
        );

        var run = RunSummary.from(runResult);
        assertThat(run.getTotal(), is(5));
        assertThat(run.getSuccess(), is(3));
        assertThat(run.getError(), is(1));
        assertThat(run.getSkipped(), is(1));
        assertThat(run.getWarn(), is(0));
        assertThat(run.getElapsedTime(), is(12.5));
        assertThat(
            run.getSlowest().stream().map(RunSummary.SlowNode::getUniqueId).toList(),
            contains("model.shop.orders", "model.shop.payments", "model.shop.customers")
        );

        var tests = TestSummary.from(runResult);
        assertThat(tests.getTotal(), is(5));
        assertThat(tests.getPass(), is(2));
        assertThat(tests.getFail(), is(1));
        assertThat(tests.getWarn(), is(1));
        assertThat(tests.getSkipped(), is(1));
        assertThat(tests.getError(), is(0));
    }

    @Test
    void emptyOrMissingResults_yieldZeroCounts() {
        var empty = RunResult.builder().results(List.of()).build();
        var missing = RunResult.builder().build();

        for (RunResult r : List.of(empty, missing)) {
            assertThat(RunSummary.from(r).getTotal(), is(0));
            assertThat(RunSummary.from(r).getSlowest().size(), is(0));
            assertThat(TestSummary.from(r).getTotal(), is(0));
        }
        assertThat(RunSummary.from(missing).getElapsedTime(), nullValue());
    }

    @Test
    void unknownOrNullStatus_countsInTotalOnly() {
        var runResult = runResult(
            result("model.shop.orders", null, 1.0),
            result("model.shop.customers", "some-future-status", 1.0),
            result("model.shop.ephemeral", "no-op", 1.0),
            result("model.shop.events", "partial success", 1.0),
            result("test.shop.unique_orders_id.c3d4", null, null),
            result(null, "success", null)
        );

        var run = RunSummary.from(runResult);
        assertThat(run.getTotal(), is(5));
        assertThat(run.getSuccess(), is(1));
        assertThat(run.getSkipped(), is(1));
        assertThat(run.getWarn(), is(1));
        assertThat(run.getError(), is(0));

        var tests = TestSummary.from(runResult);
        assertThat(tests.getTotal(), is(1));
        assertThat(tests.getPass() + tests.getFail() + tests.getWarn() + tests.getError() + tests.getSkipped(), is(0));
    }

    @Test
    void summaries_roundTripThroughJson() throws Exception {
        var runResult = runResult(result("model.shop.orders", "success", 2.0), result("test.shop.t.a1", "pass", 0.1));
        var mapper = JacksonMapper.ofJson();

        var run = RunSummary.from(runResult);
        var tests = TestSummary.from(runResult);

        assertThat(mapper.readValue(mapper.writeValueAsString(run), RunSummary.class), is(run));
        assertThat(mapper.readValue(mapper.writeValueAsString(tests), TestSummary.class), is(tests));
    }
}
