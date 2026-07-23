package io.kestra.plugin.dbt.cloud;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import org.junit.jupiter.api.Test;

import com.github.tomakehurst.wiremock.junit5.WireMockTest;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.executions.LogEntry;
import io.kestra.core.models.property.Property;
import io.kestra.core.queues.QueueFactoryInterface;
import io.kestra.core.queues.QueueInterface;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;

import jakarta.inject.Inject;
import jakarta.inject.Named;
import reactor.core.publisher.Flux;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
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
    @Named(QueueFactoryInterface.WORKERTASKLOG_NAMED)
    private QueueInterface<LogEntry> logQueue;

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
        Flux<LogEntry> receive = TestsUtils.receive(logQueue, l -> logs.add(l.getLeft()));

        // Must not throw despite the debug=true follow-up fetch failing with a 400.
        CheckStatus.Output output = checkStatus.run(runContext);

        TestsUtils.awaitLog(logs, l -> l.getMessage() != null && l.getMessage().contains("polled step output"));
        receive.blockLast();

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
        Flux<LogEntry> receive = TestsUtils.receive(logQueue, l -> logs.add(l.getLeft()));

        CheckStatus.Output output = checkStatus.run(runContext);

        TestsUtils.awaitLog(logs, l -> l.getMessage() != null && l.getMessage().contains("fuller debug tail"));
        receive.blockLast();

        assertThat(output, is(notNullValue()));
        // The fuller content only exists in the debug=true response — its presence in logs proves
        // it superseded the response collected during polling.
        assertThat(
            logs.stream().anyMatch(l -> l.getMessage() != null && l.getMessage().contains("fuller debug tail")),
            is(true)
        );
    }

    private RunContext mockRunContext(CheckStatus task) {
        var flow = TestsUtils.mockFlow();
        var execution = TestsUtils.mockExecution(flow, Map.of(), null);
        var taskRun = TestsUtils.mockTaskRun(execution, task);
        return runContextFactory.of(flow, task, execution, taskRun, false);
    }
}
