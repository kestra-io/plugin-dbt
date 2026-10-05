package io.kestra.plugin.dbt.cloud;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.github.tomakehurst.wiremock.junit5.WireMockTest;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.assets.Custom;
import io.kestra.core.models.flows.State;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;

import jakarta.inject.Inject;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
@WireMockTest(httpPort = 8089)
class CheckStatusArtifactStepTest {
    private static final String RUN_PATH = "/api/v2/accounts/123/runs/9999/";

    @Inject
    RunContextFactory runContextFactory;

    static Stream<Arguments> artifactSteps() {
        return Stream.of(
            Arguments.of(
                "run before docs", steps(
                    step(4, "Invoke dbt with `dbt run`", 10),
                    step(5, "Generate docs", 10)
                ), 4
            ),
            Arguments.of(
                "build with selection before explicit docs command", steps(
                    step(4, "Invoke dbt with `dbt build --select marts`", 10),
                    step(5, "Invoke dbt with `dbt docs generate`", 10)
                ), 4
            ),
            Arguments.of("run without docs", steps(step(4, "dbt run", 10)), 4),
            Arguments.of(
                "deps with arguments after build", steps(
                    step(4, "dbt build", 10),
                    step(5, "Invoke dbt with `dbt deps --upgrade`", 10),
                    step(6, "Invoke dbt with `dbt docs generate`", 10)
                ), 4
            ),
            Arguments.of(
                "seed after run", steps(
                    step(4, "dbt run", 10),
                    step(5, "Invoke dbt with `dbt seed`", 10),
                    step(6, "Generate docs", 10)
                ), 5
            ),
            Arguments.of(
                "snapshot after run", steps(
                    step(4, "dbt run", 10),
                    step(5, "dbt snapshot", 10),
                    step(6, "dbt docs generate --no-compile", 10)
                ), 5
            ),
            Arguments.of(
                "test without a model invocation", steps(
                    step(1, "Clone repository", 10),
                    step(2, "dbt deps", 10),
                    step(3, "dbt seed", 10),
                    step(4, "dbt test", 10)
                ), 4
            ),
            Arguments.of(
                "latest index, not response order or id", steps(
                    step(7, "Invoke dbt with `dbt build`", 10),
                    step(4, "Invoke dbt with `dbt run`", 10),
                    step(8, "Generate docs", 10)
                ), 7
            ),
            Arguments.of(
                "ignore unstarted model steps", steps(
                    step(4, "dbt run", 10),
                    step(5, "dbt build", 1),
                    step(6, "Generate docs", 10)
                ), 4
            ),
            Arguments.of(
                "other dbt invocations remain eligible", steps(
                    step(4, "dbt build", 10),
                    step(5, "Invoke dbt with `dbt run-operation build --args '{command: dbt run}'`", 10)
                ), 5
            ),
            Arguments.of(
                "ignore command names in docs arguments", steps(
                    step(4, "dbt build", 10),
                    step(5, "Invoke dbt with `dbt docs generate --vars '{command: dbt run}'`", 10)
                ), 4
            )
        );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("artifactSteps")
    void shouldUseLatestArtifactStepForBothArtifacts(String description, String runSteps, int expectedStep) throws Exception {
        stubRun(runSteps, 10);
        stubArtifacts("", 0.173);
        stubArtifacts("?step=" + expectedStep, 12.0);
        var task = task();
        var context = context(task);

        var output = task.run(context);

        assertThat(output.getRunResults(), notNullValue());
        assertThat(output.getManifest(), notNullValue());
        assertModelDuration(context, 12);
        verifyArtifacts("?step=" + expectedStep);
        verify(0, getRequestedFor(urlEqualTo(RUN_PATH + "artifacts/run_results.json")));
        verify(0, getRequestedFor(urlEqualTo(RUN_PATH + "artifacts/manifest.json")));
    }

    static Stream<Arguments> fallbackSteps() {
        return Stream.of(
            Arguments.of("empty metadata", "[]"),
            Arguments.of("missing index", """
                [{"id": 100, "name": "dbt run", "status": 10, "logs": ""}]
                """),
            Arguments.of("invalid index", steps(step(0, "dbt run", 10))),
            Arguments.of("missing name", """
                [{"id": 100, "index": 4, "status": 10, "logs": ""}]
                """),
            Arguments.of(
                "no dbt invocation", steps(
                    step(1, "Clone repository", 10),
                    step(2, "Generate docs", 10)
                )
            ),
            Arguments.of("docs-only job", steps(step(4, "Invoke dbt with `dbt docs generate`", 10))),
            Arguments.of(
                "docs-only job with deps step", steps(
                    step(1, "Clone git repository", 10),
                    step(2, "Create profile from connection Postgres", 10),
                    step(3, "Invoke dbt with `dbt deps`", 10),
                    step(4, "Invoke dbt with `dbt docs generate`", 10)
                )
            ),
            Arguments.of("deps-only job with arguments", steps(step(3, "dbt deps --upgrade", 10)))
        );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("fallbackSteps")
    void shouldKeepDefaultArtifactsWhenArtifactStepCannotBeIdentified(String description, String runSteps) throws Exception {
        stubRun(runSteps, 10);
        stubArtifacts("", 12.0);
        var task = task();
        var context = context(task);

        var output = task.run(context);

        assertThat(output.getManifest(), notNullValue());
        assertModelDuration(context, 12);
        verifyArtifacts("");
        for (var artifact : List.of("run_results.json", "manifest.json")) {
            verify(0, getRequestedFor(urlPathEqualTo(RUN_PATH + "artifacts/" + artifact)).withQueryParam("step", matching(".*")));
        }
    }

    static Stream<Arguments> testSteps() {
        return Stream.of(
            Arguments.of("successful tests without docs", "pass", 10, false),
            Arguments.of("successful tests before docs", "pass", 10, true),
            Arguments.of("failed tests without docs", "fail", 20, false),
            Arguments.of("failed tests before unstarted docs", "fail", 20, true)
        );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("testSteps")
    void shouldPreserveTestResultsAfterRun(String description, String testStatus, int runStatus, boolean generateDocs) throws Exception {
        var runStep = step(4, "Invoke dbt with `dbt run`", 10);
        var testStep = step(5, "Invoke dbt with `dbt test`", runStatus);
        stubRun(
            generateDocs
                ? steps(runStep, testStep, step(6, "Invoke dbt with `dbt docs generate`", runStatus == 10 ? 10 : 1))
                : steps(runStep, testStep),
            runStatus
        );
        stubArtifacts("", 0.173);
        stubArtifacts("?step=4", 12.0);
        stubArtifacts("?step=5", 2.0, testStatus, "test.project.not_null_orders_id");
        var task = task();
        var context = context(task);

        if (runStatus == 20) {
            var exception = assertThrows(Exception.class, () -> task.run(context));
            assertThat(exception.getMessage(), startsWith("Failed run with status"));
        } else {
            var output = task.run(context);
            assertThat(output.getRunResults(), notNullValue());
            assertThat(output.getManifest(), notNullValue());
        }

        var results = context.dynamicWorkerResults();
        assertThat(results, hasSize(1));
        var taskRun = results.getFirst().getTaskRun();
        assertThat(taskRun.getTaskId(), is("test.project.not_null_orders_id"));
        assertThat(taskRun.getState().getCurrent(), is(runStatus == 20 ? State.Type.FAILED : State.Type.SUCCESS));
        var emitted = context.assets().emitted();
        assertThat(emitted, hasSize(1));
        var metadata = ((Custom) emitted.getFirst().outputs().getFirst()).getMetadata();
        assertThat(metadata.get("dbtTestStatus"), is(testStatus));
        assertThat(metadata.get("dbtTestsTotal"), is(1));
        assertThat(metadata.get("dbtTestsFailed"), is(runStatus == 20 ? 1 : 0));
        verifyArtifacts("?step=5");
        for (var artifact : List.of("run_results.json", "manifest.json")) {
            verify(0, getRequestedFor(urlEqualTo(RUN_PATH + "artifacts/" + artifact)));
            verify(0, getRequestedFor(urlEqualTo(RUN_PATH + "artifacts/" + artifact + "?step=4")));
        }
    }

    @Test
    void shouldReadFailedModelStepBeforeFailingTheTask() throws Exception {
        stubRun(steps(step(4, "Invoke dbt with `dbt build`", 20), step(5, "Generate docs", 1)), 20);
        stubArtifacts("?step=4", 12.0, "error");
        var task = task();
        var context = context(task);

        var exception = assertThrows(Exception.class, () -> task.run(context));

        assertThat(exception.getMessage(), startsWith("Failed run with status"));
        assertModelDuration(context, 12);
        assertThat(context.dynamicWorkerResults().getFirst().getTaskRun().getState().getCurrent(), is(State.Type.FAILED));
        verifyArtifacts("?step=4");
    }

    @Test
    void shouldNotFallBackToDocsWhenModelArtifactsAreNotAvailableYet() throws Exception {
        stubRun(steps(step(4, "dbt build", 10), step(5, "Generate docs", 10)), 10);
        stubArtifacts("", 0.173);
        for (var artifact : List.of("run_results.json", "manifest.json")) {
            stubFor(
                get(urlEqualTo(RUN_PATH + "artifacts/" + artifact + "?step=4"))
                    .willReturn(aResponse().withStatus(404))
            );
        }
        var task = task();
        var context = context(task);

        var output = task.run(context);

        assertThat(output.getRunResults(), nullValue());
        assertThat(output.getManifest(), nullValue());
        assertThat(context.dynamicWorkerResults(), empty());
        verifyArtifacts("?step=4");
        verify(0, getRequestedFor(urlEqualTo(RUN_PATH + "artifacts/run_results.json")));
        verify(0, getRequestedFor(urlEqualTo(RUN_PATH + "artifacts/manifest.json")));
    }

    private void assertModelDuration(RunContext context, long seconds) {
        var results = context.dynamicWorkerResults();
        assertThat(results, hasSize(1));
        var taskRun = results.getFirst().getTaskRun();
        assertThat(taskRun.getTaskId(), is("model.project.orders"));
        var histories = taskRun.getState().getHistories();
        assertThat(histories.getFirst().getDate(), is(Instant.parse("2026-09-01T00:00:00Z")));
        assertThat(histories.getLast().getDate(), is(Instant.parse("2026-09-01T00:00:00Z").plusSeconds(seconds)));
    }

    private void verifyArtifacts(String query) {
        verify(1, getRequestedFor(urlEqualTo(RUN_PATH + "artifacts/run_results.json" + query)));
        verify(1, getRequestedFor(urlEqualTo(RUN_PATH + "artifacts/manifest.json" + query)));
    }

    private static String steps(String... steps) {
        return "[" + String.join(",", steps) + "]";
    }

    private static String step(int index, String name, int status) {
        return """
            {"id": %d, "index": %d, "name": "%s", "status": %d, "logs": ""}
            """.formatted(1000 - index, index, name, status);
    }

    private void stubRun(String steps, int status) {
        stubFor(get(urlPathEqualTo(RUN_PATH)).willReturn(okJson("""
            {"data": {"id": 9999, "status": %d, "run_steps": %s}}
            """.formatted(status, steps))));
    }

    private void stubArtifacts(String query, double duration) {
        stubArtifacts(query, duration, "success");
    }

    private void stubArtifacts(String query, double duration, String status) {
        stubArtifacts(query, duration, status, "model.project.orders");
    }

    private void stubArtifacts(String query, double duration, String status, String uniqueId) {
        stubFor(get(urlEqualTo(RUN_PATH + "artifacts/run_results.json" + query)).willReturn(okJson("""
            {
              "results": [{
                "unique_id": "%s",
                "status": "%s",
                "execution_time": %s,
                "adapter_response": {},
                "timing": [
                  {"name": "compile", "started_at": "2026-09-01T00:00:00Z", "completed_at": "2026-09-01T00:00:00.100Z"},
                  {"name": "execute", "started_at": "2026-09-01T00:00:00.100Z", "completed_at": "2026-09-01T00:00:00.110Z"}
                ]
              }],
              "elapsed_time": %s
            }
            """.formatted(uniqueId, status, duration, duration))));
        stubFor(get(urlEqualTo(RUN_PATH + "artifacts/manifest.json" + query)).willReturn(okJson("""
            {
              "metadata": {"adapter_type": "postgres"},
              "nodes": {
                "model.project.orders": {
                  "resource_type": "model", "database": "analytics", "schema": "marts",
                  "name": "orders", "unique_id": "model.project.orders", "depends_on": {"nodes": []}
                },
                "test.project.not_null_orders_id": {
                  "resource_type": "test", "name": "not_null_orders_id",
                  "unique_id": "test.project.not_null_orders_id", "depends_on": {"nodes": ["model.project.orders"]}
                }
              },
              "parent_map": {
                "model.project.orders": [],
                "test.project.not_null_orders_id": ["model.project.orders"]
              }
            }
            """)));
    }

    private CheckStatus task() {
        return CheckStatus.builder()
            .id(IdUtils.create())
            .type(CheckStatus.class.getName())
            .baseUrl(Property.ofValue("http://localhost:8089"))
            .accountId(Property.ofValue("123"))
            .token(Property.ofValue("fake-token"))
            .runId(Property.ofValue("9999"))
            .maxDuration(Property.ofValue(Duration.ofSeconds(5)))
            .parseRunResults(Property.ofValue(true))
            .build();
    }

    private RunContext context(CheckStatus task) {
        var flow = TestsUtils.mockFlow();
        var execution = TestsUtils.mockExecution(flow, Map.of(), null);
        return runContextFactory.of(flow, task, execution, TestsUtils.mockTaskRun(execution, task), false);
    }
}
