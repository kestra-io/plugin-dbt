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

    static Stream<Arguments> modelSteps() {
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
                "ignore command names in arguments", steps(
                    step(4, "dbt build", 10),
                    step(5, "Invoke dbt with `dbt run-operation build --args '{command: dbt run}'`", 10)
                ), 4
            )
        );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("modelSteps")
    void shouldUseModelStepForBothArtifacts(String description, String runSteps, int expectedStep) throws Exception {
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
                "no model invocation", steps(
                    step(1, "Clone repository", 10),
                    step(2, "dbt deps", 10),
                    step(3, "dbt seed", 10),
                    step(4, "dbt test", 10)
                )
            ),
            Arguments.of("docs-only job", steps(step(4, "Invoke dbt with `dbt docs generate`", 10)))
        );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("fallbackSteps")
    void shouldKeepDefaultArtifactsWhenModelStepCannotBeIdentified(String description, String runSteps) throws Exception {
        stubRun(runSteps, 10);
        stubArtifacts("", 12.0);
        var task = task();
        var context = context(task);

        var output = task.run(context);

        assertThat(output.getManifest(), notNullValue());
        assertModelDuration(context, 12);
        verifyArtifacts("");
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
        stubFor(get(urlEqualTo(RUN_PATH + "artifacts/run_results.json" + query)).willReturn(okJson("""
            {
              "results": [{
                "unique_id": "model.project.orders",
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
            """.formatted(status, duration, duration))));
        stubFor(get(urlEqualTo(RUN_PATH + "artifacts/manifest.json" + query)).willReturn(okJson("""
            {
              "metadata": {"adapter_type": "postgres"},
              "nodes": {
                "model.project.orders": {
                  "resource_type": "model", "database": "analytics", "schema": "marts",
                  "name": "orders", "unique_id": "model.project.orders", "depends_on": {"nodes": []}
                }
              },
              "parent_map": {"model.project.orders": []}
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
