package io.kestra.plugin.dbt.cloud;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import org.junit.jupiter.api.Test;

import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import com.github.tomakehurst.wiremock.stubbing.Scenario;

import io.kestra.core.http.client.HttpClientResponseException;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.executions.LogEntry;
import io.kestra.core.models.flows.State;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.validations.ModelValidator;
import io.kestra.core.queues.DispatchQueueInterface;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.runners.WorkerTaskResult;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;

import jakarta.inject.Inject;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
@WireMockTest(httpPort = 8089)
class CheckStatusTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private ModelValidator modelValidator;

    @Inject
    private io.kestra.plugin.dbt.TestAssetManagerFactory assetManagerFactory;

    @Inject
    private DispatchQueueInterface<LogEntry> logQueue;

    @Test
    void run() throws Exception {

        stubFor(
            post(urlEqualTo("/api/v2/accounts/123/jobs/456/run/"))
                .willReturn(okJson("""
                        { "data": { "id": 9999 } }
                    """))
        );

        stubFor(
            get(urlMatching("/api/v2/accounts/123/runs/9999/.*"))
                .willReturn(okJson("""
                        {
                          "data": {
                            "id": 9999,
                            "status": 10,
                            "status_humanized": "Success",
                            "duration_humanized": "0s",
                            "run_steps": []
                          }
                        }
                    """))
        );

        RunContext runContext = runContextFactory.of(Map.of());

        TriggerRun trigger = TriggerRun.builder()
            .id(IdUtils.create())
            .type(TriggerRun.class.getName())
            .baseUrl(Property.ofValue("http://localhost:8089"))
            .accountId(Property.ofValue("123"))
            .token(Property.ofValue("fake-token"))
            .jobId(Property.ofValue("456"))
            .wait(Property.ofValue(false))
            .build();

        TriggerRun.Output runOutput = trigger.run(runContext);

        CheckStatus checkStatus = CheckStatus.builder()
            .baseUrl(Property.ofValue("http://localhost:8089"))
            .runId(Property.ofValue(runOutput.getRunId().toString()))
            .accountId(Property.ofValue("123"))
            .token(Property.ofValue("fake-token"))
            .maxDuration(Property.ofValue(Duration.ofSeconds(5)))
            .parseRunResults(Property.ofValue(false))
            .build();

        CheckStatus.Output checkStatusOutput = checkStatus.run(runContext);

        assertThat(checkStatusOutput, is(notNullValue()));
        assertThat(checkStatusOutput.getRunResults(), is(notNullValue()));
    }

    /**
     * Regression test for defect 1: manifest.json returning 404 must not fail an otherwise
     * successful run.
     */
    @Test
    void shouldSucceedWhenManifestArtifactMissing() throws Exception {
        stubFor(
            get(urlMatching("/api/v2/accounts/123/runs/7777/\\?.*"))
                .willReturn(okJson("""
                        {
                          "data": {
                            "id": 7777,
                            "status": 10,
                            "status_humanized": "Success",
                            "duration_humanized": "1s",
                            "run_steps": []
                          }
                        }
                    """))
        );

        stubFor(
            get(urlEqualTo("/api/v2/accounts/123/runs/7777/artifacts/run_results.json"))
                .willReturn(okJson("""
                        {
                          "metadata": {},
                          "results": [],
                          "elapsed_time": 0.0
                        }
                    """))
        );

        stubFor(
            get(urlEqualTo("/api/v2/accounts/123/runs/7777/artifacts/manifest.json"))
                .willReturn(aResponse().withStatus(404).withBody("Not Found"))
        );

        RunContext runContext = runContextFactory.of(Map.of());

        CheckStatus checkStatus = CheckStatus.builder()
            .id(IdUtils.create())
            .type(CheckStatus.class.getName())
            .baseUrl(Property.ofValue("http://localhost:8089"))
            .runId(Property.ofValue("7777"))
            .accountId(Property.ofValue("123"))
            .token(Property.ofValue("fake-token"))
            .maxDuration(Property.ofValue(Duration.ofSeconds(5)))
            .parseRunResults(Property.ofValue(false))
            .build();

        CheckStatus.Output output = checkStatus.run(runContext);

        assertThat(output, is(notNullValue()));
        // run_results was present — URI must be set
        assertThat(output.getRunResults(), is(notNullValue()));
        // manifest was 404 — URI must be absent
        assertThat(output.getManifest(), is(nullValue()));
    }

    /**
     * Both artifacts return 404 (e.g. brief async upload delay). The task must still succeed.
     */
    @Test
    void shouldSucceedWhenBothArtifacts404() throws Exception {
        stubFor(
            get(urlMatching("/api/v2/accounts/123/runs/8888/\\?.*"))
                .willReturn(okJson("""
                        {
                          "data": {
                            "id": 8888,
                            "status": 10,
                            "status_humanized": "Success",
                            "duration_humanized": "1s",
                            "run_steps": []
                          }
                        }
                    """))
        );

        stubFor(
            get(urlEqualTo("/api/v2/accounts/123/runs/8888/artifacts/run_results.json"))
                .willReturn(aResponse().withStatus(404).withBody("Not Found"))
        );

        stubFor(
            get(urlEqualTo("/api/v2/accounts/123/runs/8888/artifacts/manifest.json"))
                .willReturn(aResponse().withStatus(404).withBody("Not Found"))
        );

        RunContext runContext = runContextFactory.of(Map.of());

        CheckStatus checkStatus = CheckStatus.builder()
            .id(IdUtils.create())
            .type(CheckStatus.class.getName())
            .baseUrl(Property.ofValue("http://localhost:8089"))
            .runId(Property.ofValue("8888"))
            .accountId(Property.ofValue("123"))
            .token(Property.ofValue("fake-token"))
            .maxDuration(Property.ofValue(Duration.ofSeconds(5)))
            .parseRunResults(Property.ofValue(false))
            .build();

        CheckStatus.Output output = checkStatus.run(runContext);

        assertThat(output, is(notNullValue()));
        assertThat(output.getRunResults(), is(nullValue()));
        assertThat(output.getManifest(), is(nullValue()));
    }

    /**
     * A run with integer status 20 (Error) must throw and include status_message in the message.
     * Regression test for defect 2: the verdict must use the authoritative integer status field.
     */
    @Test
    void shouldFailOnErrorStatus() throws Exception {
        stubFor(
            get(urlMatching("/api/v2/accounts/123/runs/6666/\\?.*"))
                .willReturn(okJson("""
                        {
                          "data": {
                            "id": 6666,
                            "status": 20,
                            "status_humanized": "Error",
                            "status_message": "Compilation failed in step 1",
                            "duration_humanized": "2s",
                            "run_steps": []
                          }
                        }
                    """))
        );

        RunContext runContext = runContextFactory.of(Map.of());

        CheckStatus checkStatus = CheckStatus.builder()
            .id(IdUtils.create())
            .type(CheckStatus.class.getName())
            .baseUrl(Property.ofValue("http://localhost:8089"))
            .runId(Property.ofValue("6666"))
            .accountId(Property.ofValue("123"))
            .token(Property.ofValue("fake-token"))
            .maxDuration(Property.ofValue(Duration.ofSeconds(5)))
            .build();

        var ex = assertThrows(Exception.class, () -> checkStatus.run(runContext));
        assertThat(ex.getMessage(), containsString("Compilation failed in step 1"));
    }

    /**
     * Regression test for issue #315: on a FAILED run, dbt Cloud still saves run_results.json, and
     * the task must download/parse it (emitting per-model dynamic taskruns and logs) before failing,
     * instead of throwing immediately and leaving the failed models with no per-node visibility.
     */
    @Test
    void shouldEmitModelTaskRunsOnFailedRunBeforeThrowing() throws Exception {
        stubFor(
            get(urlMatching("/api/v2/accounts/123/runs/6667/\\?.*"))
                .willReturn(okJson("""
                        {
                          "data": {
                            "id": 6667,
                            "status": 20,
                            "status_humanized": "Error",
                            "status_message": "Model failed",
                            "duration_humanized": "5s",
                            "run_steps": []
                          }
                        }
                    """))
        );

        stubFor(
            get(urlEqualTo("/api/v2/accounts/123/runs/6667/artifacts/run_results.json"))
                .willReturn(okJson("""
                        {
                          "metadata": {"dbt_version": "1.8.0"},
                          "results": [
                            {
                              "status": "error",
                              "message": "Database Error in model fct_orders",
                              "failures": 1,
                              "unique_id": "model.my_project.fct_orders",
                              "execution_time": 0.13,
                              "adapter_response": {},
                              "timing": [
                                {"name": "compile", "started_at": "2024-01-01T00:00:00Z", "completed_at": "2024-01-01T00:00:01Z"},
                                {"name": "execute", "started_at": "2024-01-01T00:00:01Z", "completed_at": "2024-01-01T00:00:02Z"}
                              ]
                            }
                          ],
                          "elapsed_time": 0.13
                        }
                    """))
        );

        stubFor(
            get(urlEqualTo("/api/v2/accounts/123/runs/6667/artifacts/manifest.json"))
                .willReturn(aResponse().withStatus(404).withBody("Not Found"))
        );

        CheckStatus checkStatus = CheckStatus.builder()
            .id(IdUtils.create())
            .type(CheckStatus.class.getName())
            .baseUrl(Property.ofValue("http://localhost:8089"))
            .runId(Property.ofValue("6667"))
            .accountId(Property.ofValue("123"))
            .token(Property.ofValue("fake-token"))
            .maxDuration(Property.ofValue(Duration.ofSeconds(5)))
            .parseRunResults(Property.ofValue(true))
            .build();

        RunContext runContext = mockRunContext(checkStatus);

        List<LogEntry> logs = new CopyOnWriteArrayList<>();
        logQueue.addListener(logs::add);

        // The run failed — the task must still throw, carrying the same message as before the fix.
        var ex = assertThrows(Exception.class, () -> checkStatus.run(runContext));
        assertThat(ex.getMessage(), containsString("Model failed"));

        // But run_results.json must have been downloaded and parsed regardless, emitting a dynamic
        // taskrun for the failed model with an ERROR state — this is what was missing before the fix.
        List<WorkerTaskResult> modelTaskRuns = runContext.dynamicWorkerResults();
        assertThat(modelTaskRuns, hasSize(1));
        assertThat(modelTaskRuns.getFirst().getTaskRun().getTaskId(), is("model.my_project.fct_orders"));
        assertThat(modelTaskRuns.getFirst().getTaskRun().getState().getCurrent(), is(State.Type.FAILED));

        // And its failure message must have been logged under that model's own dynamic taskrun.
        String modelTaskRunId = modelTaskRuns.getFirst().getTaskRun().getId();
        TestsUtils.awaitLog(logs, l -> l.getTaskRunId() != null && l.getTaskRunId().equals(modelTaskRunId));

        assertThat(
            logs.stream().anyMatch(l -> modelTaskRunId.equals(l.getTaskRunId()) && l.getMessage().contains("Database Error")),
            is(true)
        );
    }

    /**
     * Regression lock: a response carrying only status_humanized (no integer status field) must
     * still resolve the latch and succeed. This matches the stub shape used by testTriggerRunWithWait.
     */
    @Test
    void shouldSucceedWhenOnlyStatusHumanizedPresent() throws Exception {
        stubFor(
            get(urlMatching("/api/v2/accounts/123/runs/4444/\\?.*"))
                .willReturn(okJson("""
                        {
                          "data": {
                            "id": 4444,
                            "status_humanized": "Success",
                            "duration_humanized": "1s",
                            "run_steps": []
                          }
                        }
                    """))
        );

        stubFor(
            get(urlEqualTo("/api/v2/accounts/123/runs/4444/artifacts/run_results.json"))
                .willReturn(aResponse().withStatus(404).withBody("Not Found"))
        );

        stubFor(
            get(urlEqualTo("/api/v2/accounts/123/runs/4444/artifacts/manifest.json"))
                .willReturn(aResponse().withStatus(404).withBody("Not Found"))
        );

        RunContext runContext = runContextFactory.of(Map.of());

        CheckStatus checkStatus = CheckStatus.builder()
            .id(IdUtils.create())
            .type(CheckStatus.class.getName())
            .baseUrl(Property.ofValue("http://localhost:8089"))
            .runId(Property.ofValue("4444"))
            .accountId(Property.ofValue("123"))
            .token(Property.ofValue("fake-token"))
            .maxDuration(Property.ofValue(Duration.ofSeconds(5)))
            .parseRunResults(Property.ofValue(false))
            .build();

        CheckStatus.Output output = checkStatus.run(runContext);

        assertThat(output, is(notNullValue()));
    }

    /**
     * The latch and verdict must use the integer status field even when status_humanized carries an
     * unrecognized string. Integer 10 = Success regardless of the display label.
     */
    @Test
    void shouldSucceedWhenStatusHumanizedIsUnrecognized() throws Exception {
        stubFor(
            get(urlMatching("/api/v2/accounts/123/runs/5555/\\?.*"))
                .willReturn(okJson("""
                        {
                          "data": {
                            "id": 5555,
                            "status": 10,
                            "status_humanized": "Completed",
                            "duration_humanized": "3s",
                            "run_steps": []
                          }
                        }
                    """))
        );

        stubFor(
            get(urlEqualTo("/api/v2/accounts/123/runs/5555/artifacts/run_results.json"))
                .willReturn(aResponse().withStatus(404).withBody("Not Found"))
        );

        stubFor(
            get(urlEqualTo("/api/v2/accounts/123/runs/5555/artifacts/manifest.json"))
                .willReturn(aResponse().withStatus(404).withBody("Not Found"))
        );

        RunContext runContext = runContextFactory.of(Map.of());

        CheckStatus checkStatus = CheckStatus.builder()
            .id(IdUtils.create())
            .type(CheckStatus.class.getName())
            .baseUrl(Property.ofValue("http://localhost:8089"))
            .runId(Property.ofValue("5555"))
            .accountId(Property.ofValue("123"))
            .token(Property.ofValue("fake-token"))
            .maxDuration(Property.ofValue(Duration.ofSeconds(5)))
            .parseRunResults(Property.ofValue(false))
            .build();

        // Must not throw — integer status 10 is authoritative regardless of humanized label.
        CheckStatus.Output output = checkStatus.run(runContext);
        assertThat(output, is(notNullValue()));
    }

    /**
     * Regression test: a run that reaches a terminal status on the very first poll must return
     * immediately, even when a run step's truncated_debug_logs never populates. Before the fix, the
     * loop's return condition was `isEnded(data) && allLogs`, so a run step whose
     * truncated_debug_logs stays null would spin until maxDuration and throw a timeout instead of
     * surfacing the already-known terminal status.
     */
    @Test
    void shouldSucceedImmediatelyWhenTruncatedDebugLogsNeverPopulate() throws Exception {
        stubFor(
            get(urlMatching("/api/v2/accounts/123/runs/3333/\\?.*"))
                .willReturn(okJson("""
                        {
                          "data": {
                            "id": 3333,
                            "status": 10,
                            "status_humanized": "Success",
                            "duration_humanized": "1s",
                            "run_steps": [
                              {
                                "id": 1,
                                "name": "dbt run",
                                "logs": "some logs",
                                "truncated_debug_logs": null
                              }
                            ]
                          }
                        }
                    """))
        );

        stubFor(
            get(urlEqualTo("/api/v2/accounts/123/runs/3333/artifacts/run_results.json"))
                .willReturn(aResponse().withStatus(404).withBody("Not Found"))
        );

        stubFor(
            get(urlEqualTo("/api/v2/accounts/123/runs/3333/artifacts/manifest.json"))
                .willReturn(aResponse().withStatus(404).withBody("Not Found"))
        );

        RunContext runContext = runContextFactory.of(Map.of());

        CheckStatus checkStatus = CheckStatus.builder()
            .id(IdUtils.create())
            .type(CheckStatus.class.getName())
            .baseUrl(Property.ofValue("http://localhost:8089"))
            .runId(Property.ofValue("3333"))
            .accountId(Property.ofValue("123"))
            .token(Property.ofValue("fake-token"))
            .pollFrequency(Property.ofValue(Duration.ofMillis(100)))
            .maxDuration(Property.ofValue(Duration.ofSeconds(5)))
            .parseRunResults(Property.ofValue(false))
            .build();

        long start = System.currentTimeMillis();
        CheckStatus.Output output = checkStatus.run(runContext);
        long elapsed = System.currentTimeMillis() - start;

        assertThat(output, is(notNullValue()));
        // Must complete well inside maxDuration (5s), not spin until the timeout.
        assertThat(elapsed < Duration.ofSeconds(5).toMillis(), is(true));
    }

    /**
     * Regression test: the best-effort debug=true fetch done once the run is terminal must not fail
     * the task when it errors out. The run must still succeed using the response already collected
     * during polling.
     */
    @Test
    void shouldFallBackToPolledResponseWhenFinalDebugFetchFails() throws Exception {
        stubFor(
            get(urlPathEqualTo("/api/v2/accounts/123/runs/2222/"))
                .withQueryParam("include_related", notContaining("debug_logs"))
                .willReturn(okJson("""
                        {
                          "data": {
                            "id": 2222,
                            "status": 10,
                            "status_humanized": "Success",
                            "duration_humanized": "1s",
                            "run_steps": [{ "id": 1, "name": "dbt run", "logs": "polled step output" }]
                          }
                        }
                    """))
        );

        stubFor(
            get(urlPathEqualTo("/api/v2/accounts/123/runs/2222/"))
                .withQueryParam("include_related", containing("debug_logs"))
                .willReturn(aResponse().withStatus(400).withBody("Bad Request"))
        );

        stubFor(
            get(urlEqualTo("/api/v2/accounts/123/runs/2222/artifacts/run_results.json"))
                .willReturn(aResponse().withStatus(404).withBody("Not Found"))
        );

        stubFor(
            get(urlEqualTo("/api/v2/accounts/123/runs/2222/artifacts/manifest.json"))
                .willReturn(aResponse().withStatus(404).withBody("Not Found"))
        );

        CheckStatus checkStatus = CheckStatus.builder()
            .id(IdUtils.create())
            .type(CheckStatus.class.getName())
            .baseUrl(Property.ofValue("http://localhost:8089"))
            .runId(Property.ofValue("2222"))
            .accountId(Property.ofValue("123"))
            .token(Property.ofValue("fake-token"))
            .maxDuration(Property.ofValue(Duration.ofSeconds(5)))
            .parseRunResults(Property.ofValue(false))
            .build();

        RunContext runContext = mockRunContext(checkStatus);

        List<LogEntry> logs = new CopyOnWriteArrayList<>();
        logQueue.addListener(logs::add);

        // Must not throw despite the debug=true follow-up fetch failing with a 400.
        CheckStatus.Output output = checkStatus.run(runContext);

        TestsUtils.awaitLog(logs, l -> l.getMessage() != null && l.getMessage().contains("polled step output"));

        assertThat(output, is(notNullValue()));
        assertThat(
            logs.stream().anyMatch(l -> l.getMessage() != null && l.getMessage().contains("polled step output")),
            is(true)
        );
    }

    /**
     * Happy path for the best-effort debug=true fetch: when it succeeds, its fuller step logs
     * supersede the response collected during polling.
     */
    @Test
    void shouldUseDebugResponseWhenFinalDebugFetchSucceeds() throws Exception {
        stubFor(
            get(urlPathEqualTo("/api/v2/accounts/123/runs/1111/"))
                .withQueryParam("include_related", notContaining("debug_logs"))
                .willReturn(okJson("""
                        {
                          "data": {
                            "id": 1111,
                            "status": 10,
                            "status_humanized": "Success",
                            "duration_humanized": "1s",
                            "run_steps": [{ "id": 1, "name": "dbt run", "logs": "short logs" }]
                          }
                        }
                    """))
        );

        stubFor(
            get(urlPathEqualTo("/api/v2/accounts/123/runs/1111/"))
                .withQueryParam("include_related", containing("debug_logs"))
                .willReturn(okJson("""
                        {
                          "data": {
                            "id": 1111,
                            "status": 10,
                            "status_humanized": "Success",
                            "duration_humanized": "1s",
                            "run_steps": [{ "id": 1, "name": "dbt run", "logs": "short logs\\nfuller debug tail" }]
                          }
                        }
                    """))
        );

        stubFor(
            get(urlEqualTo("/api/v2/accounts/123/runs/1111/artifacts/run_results.json"))
                .willReturn(aResponse().withStatus(404).withBody("Not Found"))
        );

        stubFor(
            get(urlEqualTo("/api/v2/accounts/123/runs/1111/artifacts/manifest.json"))
                .willReturn(aResponse().withStatus(404).withBody("Not Found"))
        );

        CheckStatus checkStatus = CheckStatus.builder()
            .id(IdUtils.create())
            .type(CheckStatus.class.getName())
            .baseUrl(Property.ofValue("http://localhost:8089"))
            .runId(Property.ofValue("1111"))
            .accountId(Property.ofValue("123"))
            .token(Property.ofValue("fake-token"))
            .maxDuration(Property.ofValue(Duration.ofSeconds(5)))
            .parseRunResults(Property.ofValue(false))
            .build();

        RunContext runContext = mockRunContext(checkStatus);

        List<LogEntry> logs = new CopyOnWriteArrayList<>();
        logQueue.addListener(logs::add);

        CheckStatus.Output output = checkStatus.run(runContext);

        TestsUtils.awaitLog(logs, l -> l.getMessage() != null && l.getMessage().contains("fuller debug tail"));

        assertThat(output, is(notNullValue()));
        // The fuller content only exists in the debug=true response — its presence in logs proves
        // it superseded the response collected during polling.
        assertThat(
            logs.stream().anyMatch(l -> l.getMessage() != null && l.getMessage().contains("fuller debug tail")),
            is(true)
        );
    }

    /**
     * A transient failure while polling run status (e.g. a dbt Cloud 500 gateway page) must not fail
     * the task: the run is still healthy. The loop logs it and polls again, succeeding on the next
     * status read.
     */
    @Test
    void shouldTolerateTransientStatusReadFailure() throws Exception {
        stubFor(
            get(urlMatching("/api/v2/accounts/123/runs/9090/\\?.*"))
                .inScenario("transient-then-success")
                .whenScenarioStateIs(Scenario.STARTED)
                .willReturn(
                    aResponse()
                        .withStatus(500)
                        .withHeader("Content-Type", "text/html")
                        .withBody("<html><title>Uh oh! | dbt</title></html>")
                )
                .willSetStateTo("recovered")
        );

        stubFor(
            get(urlMatching("/api/v2/accounts/123/runs/9090/\\?.*"))
                .inScenario("transient-then-success")
                .whenScenarioStateIs("recovered")
                .willReturn(okJson("""
                        {
                          "data": {
                            "id": 9090,
                            "status": 10,
                            "status_humanized": "Success",
                            "duration_humanized": "1s",
                            "run_steps": []
                          }
                        }
                    """))
        );

        stubFor(
            get(urlEqualTo("/api/v2/accounts/123/runs/9090/artifacts/run_results.json"))
                .willReturn(aResponse().withStatus(404).withBody("Not Found"))
        );
        stubFor(
            get(urlEqualTo("/api/v2/accounts/123/runs/9090/artifacts/manifest.json"))
                .willReturn(aResponse().withStatus(404).withBody("Not Found"))
        );

        RunContext runContext = runContextFactory.of(Map.of());

        CheckStatus checkStatus = CheckStatus.builder()
            .id(IdUtils.create())
            .type(CheckStatus.class.getName())
            .baseUrl(Property.ofValue("http://localhost:8089"))
            .runId(Property.ofValue("9090"))
            .accountId(Property.ofValue("123"))
            .token(Property.ofValue("fake-token"))
            .maxRetries(Property.ofValue(1))
            .pollFrequency(Property.ofValue(Duration.ofMillis(100)))
            .maxDuration(Property.ofValue(Duration.ofSeconds(5)))
            .parseRunResults(Property.ofValue(false))
            .build();

        // Must not throw despite the first status poll returning a 500.
        CheckStatus.Output output = checkStatus.run(runContext);
        assertThat(output, is(notNullValue()));
    }

    /**
     * A non-transient error while polling (e.g. 404 for a wrong run id) must fail fast rather than
     * spin until maxDuration.
     */
    @Test
    void shouldFailFastWhenStatusReadReturnsClientError() throws Exception {
        stubFor(
            get(urlMatching("/api/v2/accounts/123/runs/9091/\\?.*"))
                .willReturn(aResponse().withStatus(404).withBody("Not Found"))
        );

        RunContext runContext = runContextFactory.of(Map.of());

        CheckStatus checkStatus = CheckStatus.builder()
            .id(IdUtils.create())
            .type(CheckStatus.class.getName())
            .baseUrl(Property.ofValue("http://localhost:8089"))
            .runId(Property.ofValue("9091"))
            .accountId(Property.ofValue("123"))
            .token(Property.ofValue("fake-token"))
            .maxRetries(Property.ofValue(1))
            .pollFrequency(Property.ofValue(Duration.ofMillis(100)))
            .maxDuration(Property.ofValue(Duration.ofSeconds(5)))
            .parseRunResults(Property.ofValue(false))
            .build();

        // Surfaces the 404 itself rather than swallowing it and spinning to a maxDuration timeout.
        var ex = assertThrows(HttpClientResponseException.class, () -> checkStatus.run(runContext));
        assertThat(ex.getResponse().getStatus().getCode(), is(404));
    }

    /** Issue #318: with only a jobId, the task resolves that job's most recent finished run. */
    @Test
    void shouldResolveLatestFinishedRunFromJobId() throws Exception {
        stubFor(
            get(urlPathEqualTo("/api/v2/accounts/123/runs/"))
                .withQueryParam("job_definition_id", equalTo("4321"))
                .withQueryParam("status__in", equalTo("[10,20,30]"))
                .withQueryParam("order_by", equalTo("-finished_at"))
                .withQueryParam("limit", equalTo("1"))
                .willReturn(okJson("""
                        {
                          "data": [
                            {
                              "id": 7777,
                              "status": 10,
                              "status_humanized": "Success",
                              "finished_at": "2026-08-31 06:00:00.000000+00:00"
                            }
                          ]
                        }
                    """))
        );

        stubFor(
            get(urlMatching("/api/v2/accounts/123/runs/7777/\\?.*"))
                .willReturn(okJson("""
                        {
                          "data": {
                            "id": 7777,
                            "status": 10,
                            "status_humanized": "Success",
                            "duration_humanized": "1s",
                            "run_steps": []
                          }
                        }
                    """))
        );

        stubFor(
            get(urlEqualTo("/api/v2/accounts/123/runs/7777/artifacts/run_results.json"))
                .willReturn(aResponse().withStatus(404).withBody("Not Found"))
        );

        stubFor(
            get(urlEqualTo("/api/v2/accounts/123/runs/7777/artifacts/manifest.json"))
                .willReturn(okJson(MANIFEST_JSON))
        );

        RunContext runContext = runContextFactory.of(Map.of());

        CheckStatus checkStatus = CheckStatus.builder()
            .id(IdUtils.create())
            .type(CheckStatus.class.getName())
            .baseUrl(Property.ofValue("http://localhost:8089"))
            .jobId(Property.ofValue("4321"))
            .accountId(Property.ofValue("123"))
            .token(Property.ofValue("fake-token"))
            .maxDuration(Property.ofValue(Duration.ofSeconds(5)))
            .parseRunResults(Property.ofValue(false))
            .build();

        CheckStatus.Output output = checkStatus.run(runContext);

        assertThat(output, is(notNullValue()));
        assertThat(output.getRunId(), is(7777L));
    }

    /**
     * A job with no finished run yet must say so, rather than failing later on a null run id.
     */
    @Test
    void shouldFailWhenJobHasNoFinishedRun() {
        stubFor(
            get(urlPathEqualTo("/api/v2/accounts/123/runs/"))
                .withQueryParam("job_definition_id", equalTo("4322"))
                .willReturn(okJson("""
                        { "data": [] }
                    """))
        );

        RunContext runContext = runContextFactory.of(Map.of());

        CheckStatus checkStatus = CheckStatus.builder()
            .id(IdUtils.create())
            .type(CheckStatus.class.getName())
            .baseUrl(Property.ofValue("http://localhost:8089"))
            .jobId(Property.ofValue("4322"))
            .accountId(Property.ofValue("123"))
            .token(Property.ofValue("fake-token"))
            .maxDuration(Property.ofValue(Duration.ofSeconds(5)))
            .build();

        var ex = assertThrows(IllegalStateException.class, () -> checkStatus.run(runContext));
        assertThat(ex.getMessage(), containsString("4322"));
    }

    /** A refresh does not own the run it reads, so with the flag off a failed run must not fail the task. */
    @Test
    void shouldNotThrowOnErrorStatusWhenFailOnUnsuccessfulIsFalse() throws Exception {
        stubFor(
            get(urlMatching("/api/v2/accounts/123/runs/6667/\\?.*"))
                .willReturn(okJson("""
                        {
                          "data": {
                            "id": 6667,
                            "status": 20,
                            "status_humanized": "Error",
                            "status_message": "Compilation failed in step 1",
                            "duration_humanized": "2s",
                            "run_steps": []
                          }
                        }
                    """))
        );

        stubFor(
            get(urlEqualTo("/api/v2/accounts/123/runs/6667/artifacts/run_results.json"))
                .willReturn(aResponse().withStatus(404).withBody("Not Found"))
        );

        stubFor(
            get(urlEqualTo("/api/v2/accounts/123/runs/6667/artifacts/manifest.json"))
                .willReturn(okJson(MANIFEST_JSON))
        );

        RunContext runContext = runContextFactory.of(Map.of());

        CheckStatus checkStatus = CheckStatus.builder()
            .id(IdUtils.create())
            .type(CheckStatus.class.getName())
            .baseUrl(Property.ofValue("http://localhost:8089"))
            .runId(Property.ofValue("6667"))
            .accountId(Property.ofValue("123"))
            .token(Property.ofValue("fake-token"))
            .maxDuration(Property.ofValue(Duration.ofSeconds(5)))
            .parseRunResults(Property.ofValue(false))
            .failOnUnsuccessful(Property.ofValue(false))
            .build();

        CheckStatus.Output output = checkStatus.run(runContext);

        assertThat(output, is(notNullValue()));
        assertThat(output.getRunId(), is(6667L));
    }

    /** The lookup keeps failed runs: dbt writes the manifest at parse time, so it is still complete. */
    @Test
    void shouldResolveFailedRunFromJobIdWithoutThrowingWhenFailOnUnsuccessfulIsFalse() throws Exception {
        stubFor(
            get(urlPathEqualTo("/api/v2/accounts/123/runs/"))
                .withQueryParam("job_definition_id", equalTo("4323"))
                .withQueryParam("status__in", equalTo("[10,20,30]"))
                .willReturn(okJson("""
                        {
                          "data": [
                            {
                              "id": 5555,
                              "status": 20,
                              "status_humanized": "Error",
                              "finished_at": "2026-08-31 06:00:00.000000+00:00"
                            }
                          ]
                        }
                    """))
        );

        stubFor(
            get(urlMatching("/api/v2/accounts/123/runs/5555/\\?.*"))
                .willReturn(okJson("""
                        {
                          "data": {
                            "id": 5555,
                            "status": 20,
                            "status_humanized": "Error",
                            "status_message": "Compilation failed in step 1",
                            "duration_humanized": "2s",
                            "run_steps": []
                          }
                        }
                    """))
        );

        stubFor(
            get(urlEqualTo("/api/v2/accounts/123/runs/5555/artifacts/run_results.json"))
                .willReturn(aResponse().withStatus(404).withBody("Not Found"))
        );

        stubFor(
            get(urlEqualTo("/api/v2/accounts/123/runs/5555/artifacts/manifest.json"))
                .willReturn(okJson(MANIFEST_JSON))
        );

        RunContext runContext = runContextFactory.of(Map.of());

        CheckStatus checkStatus = CheckStatus.builder()
            .id(IdUtils.create())
            .type(CheckStatus.class.getName())
            .baseUrl(Property.ofValue("http://localhost:8089"))
            .jobId(Property.ofValue("4323"))
            .accountId(Property.ofValue("123"))
            .token(Property.ofValue("fake-token"))
            .maxDuration(Property.ofValue(Duration.ofSeconds(5)))
            .parseRunResults(Property.ofValue(false))
            .failOnUnsuccessful(Property.ofValue(false))
            .build();

        CheckStatus.Output output = checkStatus.run(runContext);

        assertThat(output, is(notNullValue()));
        assertThat(output.getRunId(), is(5555L));
    }

    /** Exactly one selector must be set, caught at flow validation rather than at runtime. */
    @Test
    void shouldRejectMoreThanOneRunSelector() {
        assertRejected(checkStatusBuilder().runId(Property.ofValue("1")).jobId(Property.ofValue("2")).build());
        assertRejected(checkStatusBuilder().runId(Property.ofValue("1")).environmentId(Property.ofValue("3")).build());
        assertRejected(checkStatusBuilder().jobId(Property.ofValue("2")).environmentId(Property.ofValue("3")).build());
        assertRejected(
            checkStatusBuilder()
                .runId(Property.ofValue("1"))
                .jobId(Property.ofValue("2"))
                .environmentId(Property.ofValue("3"))
                .build()
        );
    }

    @Test
    void shouldRejectNoRunSelector() {
        assertRejected(checkStatusBuilder().build());
    }

    @Test
    void shouldAcceptExactlyOneRunSelector() {
        assertThat(modelValidator.isValid(checkStatusBuilder().runId(Property.ofValue("1")).build()).isPresent(), is(false));
        assertThat(modelValidator.isValid(checkStatusBuilder().jobId(Property.ofValue("2")).build()).isPresent(), is(false));
        assertThat(modelValidator.isValid(checkStatusBuilder().environmentId(Property.ofValue("3")).build()).isPresent(), is(false));
    }

    private void assertRejected(CheckStatus task) {
        var validation = modelValidator.isValid(task);
        assertThat(validation.isPresent(), is(true));
        assertThat(validation.get().getMessage(), containsString("Exactly one of"));
    }

    /** An environmentId reads the newest finished run anywhere in the environment. */
    @Test
    void shouldResolveLatestFinishedRunFromEnvironmentId() throws Exception {
        stubFor(
            get(urlPathEqualTo("/api/v2/accounts/123/runs/"))
                .withQueryParam("environment_id", equalTo("77"))
                .withQueryParam("status__in", equalTo("[10,20,30]"))
                .withQueryParam("order_by", equalTo("-finished_at"))
                .withQueryParam("limit", equalTo("1"))
                .willReturn(okJson("""
                        {
                          "data": [
                            {
                              "id": 7790,
                              "status": 10,
                              "status_humanized": "Success",
                              "finished_at": "2026-08-31 06:00:00.000000+00:00"
                            }
                          ]
                        }
                    """))
        );
        stubFinishedRun(7790, 10, "Success");

        CheckStatus checkStatus = CheckStatus.builder()
            .id(IdUtils.create())
            .type(CheckStatus.class.getName())
            .baseUrl(Property.ofValue("http://localhost:8089"))
            .environmentId(Property.ofValue("77"))
            .accountId(Property.ofValue("123"))
            .token(Property.ofValue("fake-token"))
            .maxDuration(Property.ofValue(Duration.ofSeconds(5)))
            .parseRunResults(Property.ofValue(false))
            .build();
        RunContext runContext = mockRunContext(checkStatus);

        CheckStatus.Output first = checkStatus.run(runContext);
        assertThat(first.getRunId(), is(7790L));
        assertThat(first.isLineageEmitted(), is(true));

        // The skip guard covers the environment path too.
        assertThat(checkStatus.run(runContext).isLineageEmitted(), is(false));
    }

    /** The watermark is keyed on the selector, so a job and an environment must not suppress each other. */
    @Test
    void shouldNotShareWatermarkBetweenJobAndEnvironmentSelectors() throws Exception {
        stubLatestFinishedRun("4327", 7791);
        stubFor(
            get(urlPathEqualTo("/api/v2/accounts/123/runs/"))
                .withQueryParam("environment_id", equalTo("78"))
                .willReturn(okJson("""
                        {
                          "data": [
                            {
                              "id": 7791,
                              "status": 10,
                              "status_humanized": "Success",
                              "finished_at": "2026-08-31 06:00:00.000000+00:00"
                            }
                          ]
                        }
                    """))
        );

        CheckStatus byJob = refresherFor("4327").build();
        assertThat(byJob.run(mockRunContext(byJob)).isLineageEmitted(), is(true));

        CheckStatus byEnvironment = CheckStatus.builder()
            .id(IdUtils.create())
            .type(CheckStatus.class.getName())
            .baseUrl(Property.ofValue("http://localhost:8089"))
            .environmentId(Property.ofValue("78"))
            .accountId(Property.ofValue("123"))
            .token(Property.ofValue("fake-token"))
            .maxDuration(Property.ofValue(Duration.ofSeconds(5)))
            .parseRunResults(Property.ofValue(false))
            .build();

        // Same run id, different selector: must still be read.
        assertThat(byEnvironment.run(mockRunContext(byEnvironment)).isLineageEmitted(), is(true));
    }

    /**
     * Issue #318: a refresh resolves the same run on every tick. The second read must not append an
     * identical lineage event, but must still return its usual outputs.
     */
    @Test
    void shouldNotReEmitLineageForAnUnchangedRunButStillReturnItsOutputs() throws Exception {
        stubLatestFinishedRun("4324", 7778);

        CheckStatus checkStatus = refresherFor("4324").build();
        RunContext runContext = mockRunContext(checkStatus);

        CheckStatus.Output first = checkStatus.run(runContext);
        assertThat(first.getRunId(), is(7778L));
        assertThat(first.isLineageEmitted(), is(true));
        assertThat(first.getManifest(), is(notNullValue()));
        assertThat(first.getAssets(), containsInAnyOrder("analytics.staging.stg_orders", "analytics.marts.fct_orders"));

        CheckStatus.Output second = checkStatus.run(runContext);
        assertThat(second.getRunId(), is(7778L));

        assertThat(second.isLineageEmitted(), is(false));

        // The output contract is unchanged, which an early return would have broken.
        assertThat(second.getManifest(), is(notNullValue()));
        assertThat(second.getAssets(), containsInAnyOrder("analytics.staging.stg_orders", "analytics.marts.fct_orders"));
    }

    /**
     * dbt Cloud uploads artifacts asynchronously, so a tick that catches a run before they land emitted
     * nothing and must retry rather than record the run as done.
     */
    @Test
    void shouldNotRecordAsProcessedWhenTheManifestIsNotAvailableYet() throws Exception {
        stubFor(
            get(urlPathEqualTo("/api/v2/accounts/123/runs/"))
                .withQueryParam("job_definition_id", equalTo("4329"))
                .willReturn(okJson("""
                        {
                          "data": [
                            {
                              "id": 7796,
                              "status": 10,
                              "status_humanized": "Success",
                              "finished_at": "2026-08-31 06:00:00.000000+00:00"
                            }
                          ]
                        }
                    """))
        );
        stubFor(
            get(urlMatching("/api/v2/accounts/123/runs/7796/\\?.*"))
                .willReturn(okJson("""
                        {
                          "data": {
                            "id": 7796,
                            "status": 10,
                            "status_humanized": "Success",
                            "duration_humanized": "1s",
                            "run_steps": []
                          }
                        }
                    """))
        );
        // Artifacts not uploaded yet.
        stubFor(
            get(urlMatching("/api/v2/accounts/123/runs/7796/artifacts/.*"))
                .willReturn(aResponse().withStatus(404).withBody("Not Found"))
        );

        CheckStatus checkStatus = refresherFor("4329").build();
        RunContext runContext = mockRunContext(checkStatus);

        assertThat(checkStatus.run(runContext).isLineageEmitted(), is(false));

        // No watermark was written, so once dbt finishes uploading the next tick emits rather than skipping.
        stubFor(
            get(urlEqualTo("/api/v2/accounts/123/runs/7796/artifacts/manifest.json"))
                .willReturn(okJson(MANIFEST_JSON))
        );

        CheckStatus.Output afterUpload = checkStatus.run(runContext);
        assertThat(afterUpload.isLineageEmitted(), is(true));
        assertThat(afterUpload.getAssets(), hasSize(2));
    }

    /**
     * A new run must still emit, otherwise lineage would freeze at the first run seen.
     */
    @Test
    void shouldReadRunAgainWhenTheJobHasANewerRun() throws Exception {
        stubLatestFinishedRun("4325", 7779);

        CheckStatus checkStatus = refresherFor("4325").build();
        RunContext runContext = mockRunContext(checkStatus);

        assertThat(checkStatus.run(runContext).isLineageEmitted(), is(true));
        assertThat(checkStatus.run(runContext).isLineageEmitted(), is(false));

        // dbt Cloud finishes a newer run for the same job.
        stubLatestFinishedRun("4325", 7780);

        CheckStatus.Output afterNewRun = checkStatus.run(runContext);
        assertThat(afterNewRun.getRunId(), is(7780L));
        assertThat(afterNewRun.isLineageEmitted(), is(true));
    }

    /**
     * An explicit runId names one run to read, so the guard must never suppress its emit.
     */
    @Test
    void shouldAlwaysEmitWhenAnExplicitRunIdIsGiven() throws Exception {
        stubFinishedRun(7782, 10, "Success");

        CheckStatus checkStatus = CheckStatus.builder()
            .id(IdUtils.create())
            .type(CheckStatus.class.getName())
            .baseUrl(Property.ofValue("http://localhost:8089"))
            .runId(Property.ofValue("7782"))
            .accountId(Property.ofValue("123"))
            .token(Property.ofValue("fake-token"))
            .maxDuration(Property.ofValue(Duration.ofSeconds(5)))
            .parseRunResults(Property.ofValue(false))
            .build();
        RunContext runContext = mockRunContext(checkStatus);

        assertThat(checkStatus.run(runContext).isLineageEmitted(), is(true));
        assertThat(checkStatus.run(runContext).isLineageEmitted(), is(true));
    }

    /**
     * Asserted on the emitter, since this path throws. A failed run emits lineage then throws, so the
     * watermark has to be written before the throw. The task still fails on both ticks.
     */
    @Test
    void shouldRecordWatermarkForAFailedRunSoLineageIsNotReEmittedEveryTick() throws Exception {
        stubFor(
            get(urlPathEqualTo("/api/v2/accounts/123/runs/"))
                .withQueryParam("job_definition_id", equalTo("4328"))
                .willReturn(okJson("""
                        {
                          "data": [
                            {
                              "id": 7795,
                              "status": 20,
                              "status_humanized": "Error",
                              "finished_at": "2026-08-31 06:00:00.000000+00:00"
                            }
                          ]
                        }
                    """))
        );
        stubFinishedRun(7795, 20, "Error");

        // failOnUnsuccessful deliberately left at its default of true.
        CheckStatus checkStatus = refresherFor("4328").build();
        RunContext runContext = mockRunContext(checkStatus);

        assetManagerFactory.clear();

        // First tick reports the failed run, and emits its lineage on the way out.
        var first = assertThrows(Exception.class, () -> checkStatus.run(runContext));
        assertThat(first.getMessage(), containsString("Error"));
        assertThat(assetManagerFactory.allEmitted(), hasSize(2));

        // Second tick still fails, because the run is still unsuccessful and failOnUnsuccessful is on.
        // But the lineage must not be emitted a second time for a run that has not changed.
        var second = assertThrows(Exception.class, () -> checkStatus.run(runContext));
        assertThat(second.getMessage(), containsString("Error"));
        assertThat(assetManagerFactory.allEmitted(), hasSize(2));
    }

    private CheckStatus.CheckStatusBuilder<?, ?> refresherFor(String jobId) {
        return CheckStatus.builder()
            // Random per test run: the watermark is keyed on the task id and outlives the JVM.
            .id(IdUtils.create())
            .type(CheckStatus.class.getName())
            .baseUrl(Property.ofValue("http://localhost:8089"))
            .jobId(Property.ofValue(jobId))
            .accountId(Property.ofValue("123"))
            .token(Property.ofValue("fake-token"))
            .maxDuration(Property.ofValue(Duration.ofSeconds(5)))
            .parseRunResults(Property.ofValue(false));
    }

    private void stubLatestFinishedRun(String jobId, long runId) {
        stubFor(
            get(urlPathEqualTo("/api/v2/accounts/123/runs/"))
                .withQueryParam("job_definition_id", equalTo(jobId))
                .willReturn(okJson("""
                        {
                          "data": [
                            {
                              "id": %d,
                              "status": 10,
                              "status_humanized": "Success",
                              "finished_at": "2026-08-31 06:00:00.000000+00:00"
                            }
                          ]
                        }
                    """.formatted(runId)))
        );

        stubFinishedRun(runId, 10, "Success");
    }

    private void stubFinishedRun(long runId, int status, String humanized) {
        stubFor(
            get(urlMatching("/api/v2/accounts/123/runs/" + runId + "/\\?.*"))
                .willReturn(okJson("""
                        {
                          "data": {
                            "id": %d,
                            "status": %d,
                            "status_humanized": "%s",
                            "duration_humanized": "1s",
                            "run_steps": []
                          }
                        }
                    """.formatted(runId, status, humanized)))
        );

        stubFor(
            get(urlEqualTo("/api/v2/accounts/123/runs/" + runId + "/artifacts/run_results.json"))
                .willReturn(aResponse().withStatus(404).withBody("Not Found"))
        );

        // A real manifest, so lineage lands and the watermark is written.
        stubFor(
            get(urlEqualTo("/api/v2/accounts/123/runs/" + runId + "/artifacts/manifest.json"))
                .willReturn(okJson(MANIFEST_JSON))
        );
    }

    private static final String MANIFEST_JSON = """
        {
          "metadata": { "adapter_type": "postgres" },
          "nodes": {
            "model.analytics.stg_orders": {
              "resource_type": "model",
              "database": "analytics",
              "schema": "staging",
              "name": "stg_orders",
              "unique_id": "model.analytics.stg_orders",
              "depends_on": { "nodes": [] }
            },
            "model.analytics.fct_orders": {
              "resource_type": "model",
              "database": "analytics",
              "schema": "marts",
              "name": "fct_orders",
              "unique_id": "model.analytics.fct_orders",
              "depends_on": { "nodes": ["model.analytics.stg_orders"] }
            }
          },
          "parent_map": {
            "model.analytics.stg_orders": [],
            "model.analytics.fct_orders": ["model.analytics.stg_orders"]
          }
        }
        """;

    private CheckStatus.CheckStatusBuilder<?, ?> checkStatusBuilder() {
        return CheckStatus.builder()
            .id(IdUtils.create())
            .type(CheckStatus.class.getName())
            .accountId(Property.ofValue("123"))
            .token(Property.ofValue("fake-token"));
    }

    private RunContext mockRunContext(CheckStatus task) {
        var flow = TestsUtils.mockFlow();
        var execution = TestsUtils.mockExecution(flow, Map.of(), null);
        var taskRun = TestsUtils.mockTaskRun(execution, task);
        return runContextFactory.of(flow, task, execution, taskRun, false);
    }
}
