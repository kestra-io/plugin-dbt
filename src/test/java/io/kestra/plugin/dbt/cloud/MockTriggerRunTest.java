package io.kestra.plugin.dbt.cloud;

import java.time.Duration;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.github.tomakehurst.wiremock.junit5.WireMockTest;

import io.kestra.core.http.client.HttpClientResponseException;
import io.kestra.core.http.client.configurations.HttpConfiguration;
import io.kestra.core.http.client.configurations.TimeoutConfiguration;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.RetryUtils;
import io.kestra.core.utils.TestsUtils;

import jakarta.inject.Inject;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static com.github.tomakehurst.wiremock.stubbing.Scenario.STARTED;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
@WireMockTest(httpPort = 28181)
class MockTriggerRunTest {

    @Inject
    RunContextFactory runContextFactory;

    @Test
    void testTriggerRun() throws Exception {

        stubFor(
            post(urlEqualTo("/api/v2/accounts/123/jobs/456/run/"))
                .willReturn(
                    aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody("{\"data\":{\"id\":789}}")
                )
        );

        TriggerRun task = TriggerRun.builder()
            .id(IdUtils.create())
            .type(TriggerRun.class.getName())
            .accountId(Property.ofValue("123"))
            .jobId(Property.ofValue("456"))
            .token(Property.ofValue("my-token"))
            .baseUrl(Property.ofValue("http://localhost:28181"))
            .wait(Property.ofValue(false))
            .build();

        RunContext runContext = runContextFactory.of(Map.of());
        TriggerRun.Output output = task.run(runContext);

        assertThat(output, is(notNullValue()));
        assertThat(output.getRunId(), is(789L));
        // reattach defaults off, so the cause must be left untouched (no taskrun tag) for backward compatibility
        verify(
            postRequestedFor(urlEqualTo("/api/v2/accounts/123/jobs/456/run/"))
                .withRequestBody(matchingJsonPath("$.cause", equalTo("Triggered by Kestra.")))
        );
        // and no lookup of existing runs is ever made when reattach is off
        verify(0, getRequestedFor(urlPathEqualTo("/api/v2/accounts/123/runs/")));
    }

    @Test
    void testTriggerRunWithWait() throws Exception {

        stubFor(
            post(urlEqualTo("/api/v2/accounts/123/jobs/456/run/"))
                .willReturn(okJson("{\"data\":{\"id\":789}}"))
        );

        stubFor(
            get(urlPathEqualTo("/api/v2/accounts/123/runs/789/"))
                .withQueryParam("include_related", matching(".*run_steps.*"))
                .willReturn(okJson("""
                        {
                          "data": {
                            "id": 789,
                            "status_humanized": "Success",
                            "duration_humanized": "1m",
                            "run_steps": [{
                              "id": 1,
                              "name": "step1",
                              "logs": "log line 1",
                              "truncated_debug_logs": "truncated"
                            }]
                          }
                        }
                    """))
        );

        // stub for run result artifacts
        stubFor(
            get(urlEqualTo("/api/v2/accounts/123/runs/789/artifacts/run_results.json"))
                .willReturn(okJson("""
                    {
                      "metadata": {},
                      "results": [
                        {
                          "status": "success",
                          "unique_id": "model.my_model",
                          "execution_time": 1.23,
                          "adapter_response": {},
                          "message": "Success",
                          "failures": 0,
                          "thread_id": "Thread-1",
                          "timing": []
                        }
                      ]
                    }
                    """))
        );

        // stub for run manifest artifacts
        stubFor(
            get(urlEqualTo("/api/v2/accounts/123/runs/789/artifacts/manifest.json"))
                .willReturn(okJson("{\"nodes\": {}}"))
        );

        TriggerRun task = TriggerRun.builder()
            .id(IdUtils.create())
            .type(TriggerRun.class.getName())
            .accountId(Property.ofValue("123"))
            .jobId(Property.ofValue("456"))
            .token(Property.ofValue("demo"))
            .parseRunResults(Property.ofValue(true))
            .baseUrl(Property.ofValue("http://localhost:28181"))
            .wait(Property.ofValue(true))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());
        TriggerRun.Output output = task.run(runContext);

        assertThat(output, is(notNullValue()));
        assertThat(output.getRunId(), is(789L));
        assertThat(output.getRunResults().toString(), containsString("kestra://"));
        assertThat(output.getManifest(), is(notNullValue()));
    }

    @Test
    void testReattachToInFlightRun() throws Exception {
        TriggerRun task = TriggerRun.builder()
            .id(IdUtils.create())
            .type(TriggerRun.class.getName())
            .accountId(Property.ofValue("123"))
            .jobId(Property.ofValue("456"))
            .token(Property.ofValue("my-token"))
            .baseUrl(Property.ofValue("http://localhost:28181"))
            .reattach(Property.ofValue(true))
            .wait(Property.ofValue(false))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());
        String tag = "[taskrun:" + runContext.taskRunInfo().taskRunId() + "]";

        // in-flight run that THIS taskrun started (its cause carries our tag)
        stubFor(
            get(urlPathEqualTo("/api/v2/accounts/123/runs/"))
                .withQueryParam("job_definition_id", equalTo("456"))
                .withQueryParam("include_related", matching(".*trigger.*"))
                .willReturn(okJson("{\"data\":[{\"id\":789,\"status\":3,\"trigger\":{\"cause\":\"Triggered by Kestra. " + tag + "\"}}]}"))
        );

        TriggerRun.Output output = task.run(runContext);

        assertThat(output.getRunId(), is(789L));
        verify(0, postRequestedFor(urlEqualTo("/api/v2/accounts/123/jobs/456/run/")));
    }

    @Test
    void testReattachSkipsForeignRun() throws Exception {
        TriggerRun task = TriggerRun.builder()
            .id(IdUtils.create())
            .type(TriggerRun.class.getName())
            .accountId(Property.ofValue("123"))
            .jobId(Property.ofValue("456"))
            .token(Property.ofValue("my-token"))
            .baseUrl(Property.ofValue("http://localhost:28181"))
            .reattach(Property.ofValue(true))
            .wait(Property.ofValue(false))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());

        // an in-flight run of the job, but NOT started by this taskrun (cause has no matching tag)
        stubFor(
            get(urlPathEqualTo("/api/v2/accounts/123/runs/"))
                .withQueryParam("job_definition_id", equalTo("456"))
                .willReturn(okJson("{\"data\":[{\"id\":999,\"status\":3,\"trigger\":{\"cause\":\"Triggered manually\"}}]}"))
        );
        stubFor(
            post(urlEqualTo("/api/v2/accounts/123/jobs/456/run/"))
                .willReturn(okJson("{\"data\":{\"id\":790}}"))
        );

        TriggerRun.Output output = task.run(runContext);

        // did not attach to the foreign run 999, triggered our own instead
        assertThat(output.getRunId(), is(790L));
        verify(1, postRequestedFor(urlEqualTo("/api/v2/accounts/123/jobs/456/run/")));
    }

    @Test
    void testReattachTriggersWhenNoRunInFlight() throws Exception {
        stubFor(
            get(urlPathEqualTo("/api/v2/accounts/123/runs/"))
                .withQueryParam("job_definition_id", equalTo("456"))
                .willReturn(okJson("{\"data\":[]}"))
        );

        stubFor(
            post(urlEqualTo("/api/v2/accounts/123/jobs/456/run/"))
                .willReturn(okJson("{\"data\":{\"id\":790}}"))
        );

        TriggerRun task = TriggerRun.builder()
            .id(IdUtils.create())
            .type(TriggerRun.class.getName())
            .accountId(Property.ofValue("123"))
            .jobId(Property.ofValue("456"))
            .token(Property.ofValue("my-token"))
            .baseUrl(Property.ofValue("http://localhost:28181"))
            .reattach(Property.ofValue(true))
            .wait(Property.ofValue(false))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());
        TriggerRun.Output output = task.run(runContext);

        assertThat(output.getRunId(), is(790L));
        // reattach on, so the freshly triggered run carries this taskrun's tag in its cause
        verify(
            postRequestedFor(urlEqualTo("/api/v2/accounts/123/jobs/456/run/"))
                .withRequestBody(matchingJsonPath("$.cause", containing("[taskrun:")))
        );
    }

    @Test
    void testReattachFailsLoudWhenLookupErrors() throws Exception {
        // The reattach lookup fails: the task must fail loudly, not silently trigger a duplicate.
        stubFor(
            get(urlPathEqualTo("/api/v2/accounts/123/runs/"))
                .withQueryParam("job_definition_id", equalTo("456"))
                .willReturn(aResponse().withStatus(500).withHeader("Content-Type", "application/json"))
        );

        TriggerRun task = TriggerRun.builder()
            .id(IdUtils.create())
            .type(TriggerRun.class.getName())
            .accountId(Property.ofValue("123"))
            .jobId(Property.ofValue("456"))
            .token(Property.ofValue("my-token"))
            .baseUrl(Property.ofValue("http://localhost:28181"))
            .reattach(Property.ofValue(true))
            .wait(Property.ofValue(false))
            .maxRetries(Property.ofValue(1))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());

        // The 500 is retried (GET is read-only), retries are exhausted, and the failure surfaces
        // as RetryFailed wrapping the HTTP error, never a silent success that triggers a duplicate.
        assertThatThrownBy(() -> task.run(runContext))
            .isInstanceOf(RetryUtils.RetryFailed.class)
            .hasRootCauseInstanceOf(HttpClientResponseException.class);
        verify(0, postRequestedFor(urlEqualTo("/api/v2/accounts/123/jobs/456/run/")));
    }

    @Test
    void shouldThrowOnNon200Response() {
        stubFor(
            post(urlEqualTo("/api/v2/accounts/123/jobs/456/run/"))
                .willReturn(
                    aResponse()
                        .withStatus(500)
                        .withHeader("Content-Type", "application/json")
                )
        );

        RunContext runContext = runContextFactory.of(Map.of());

        TriggerRun task = TriggerRun.builder()
            .accountId(Property.ofValue("123"))
            .jobId(Property.ofValue("456"))
            .token(Property.ofValue("demo"))
            .baseUrl(Property.ofValue("http://localhost:28181"))
            .wait(Property.ofValue(false))
            .build();

        assertThatThrownBy(() -> task.run(runContext))
            .isInstanceOf(HttpClientResponseException.class)
            .hasMessageContaining("Failed http request with response code '500'");
    }

    @Test
    void testAdoptsRunAfterAmbiguousTriggerTimeout() throws Exception {
        // A short read timeout lets the delayed trigger response below surface as a SocketTimeoutException,
        // the same ambiguous failure dbt Cloud produces when the response is lost on the wire mid-flight.
        HttpConfiguration shortTimeout = HttpConfiguration.builder()
            .timeout(TimeoutConfiguration.builder().readIdleTimeout(Property.ofValue(Duration.ofMillis(300))).build())
            .build();

        TriggerRun task = TriggerRun.builder()
            .id(IdUtils.create())
            .type(TriggerRun.class.getName())
            .accountId(Property.ofValue("123"))
            .jobId(Property.ofValue("456"))
            .token(Property.ofValue("my-token"))
            .baseUrl(Property.ofValue("http://localhost:28181"))
            .reattach(Property.ofValue(true))
            .wait(Property.ofValue(false))
            .options(shortTimeout)
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());
        String tag = "[taskrun:" + runContext.taskRunInfo().taskRunId() + "]";

        // no run started by this taskrun yet: the start-of-run reattach lookup finds nothing, so the trigger fires
        stubFor(
            get(urlPathEqualTo("/api/v2/accounts/123/runs/"))
                .inScenario("adopt-on-timeout")
                .whenScenarioStateIs(STARTED)
                .withQueryParam("job_definition_id", equalTo("456"))
                .willReturn(okJson("{\"data\":[]}"))
                .willSetStateTo("run created")
        );

        // the trigger POST reaches dbt Cloud (which creates the run) but its response is lost on the wire
        stubFor(
            post(urlEqualTo("/api/v2/accounts/123/jobs/456/run/"))
                .willReturn(
                    aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody("{\"data\":{\"id\":789}}")
                        .withFixedDelay(1000)
                )
        );

        // the confirm-and-adopt lookup after the timeout now finds the run this taskrun created
        stubFor(
            get(urlPathEqualTo("/api/v2/accounts/123/runs/"))
                .inScenario("adopt-on-timeout")
                .whenScenarioStateIs("run created")
                .withQueryParam("job_definition_id", equalTo("456"))
                .willReturn(okJson("{\"data\":[{\"id\":789,\"status\":10,\"trigger\":{\"cause\":\"Triggered by Kestra. " + tag + "\"}}]}"))
        );

        TriggerRun.Output output = task.run(runContext);

        assertThat(output.getRunId(), is(789L));
        verify(1, postRequestedFor(urlEqualTo("/api/v2/accounts/123/jobs/456/run/")));
    }

    @Test
    void testAdoptsRunAfterAmbiguousTriggerTimeoutEvenWhenItFailed() throws Exception {
        // Same ambiguous-timeout scenario, but the run created by the timed-out POST turns out to have
        // FAILED (status 20). Unlike the start-of-run check, this inline confirm-and-adopt path must
        // still adopt it: it was just created by this attempt's own POST, so its real (failed) outcome
        // has to be reported for this attempt rather than a misleading timeout.
        HttpConfiguration shortTimeout = HttpConfiguration.builder()
            .timeout(TimeoutConfiguration.builder().readIdleTimeout(Property.ofValue(Duration.ofMillis(300))).build())
            .build();

        TriggerRun task = TriggerRun.builder()
            .id(IdUtils.create())
            .type(TriggerRun.class.getName())
            .accountId(Property.ofValue("123"))
            .jobId(Property.ofValue("456"))
            .token(Property.ofValue("my-token"))
            .baseUrl(Property.ofValue("http://localhost:28181"))
            .reattach(Property.ofValue(true))
            .wait(Property.ofValue(false))
            .options(shortTimeout)
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());
        String tag = "[taskrun:" + runContext.taskRunInfo().taskRunId() + "]";

        stubFor(
            get(urlPathEqualTo("/api/v2/accounts/123/runs/"))
                .inScenario("adopt-on-timeout-failed")
                .whenScenarioStateIs(STARTED)
                .withQueryParam("job_definition_id", equalTo("456"))
                .willReturn(okJson("{\"data\":[]}"))
                .willSetStateTo("run created")
        );

        stubFor(
            post(urlEqualTo("/api/v2/accounts/123/jobs/456/run/"))
                .willReturn(
                    aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody("{\"data\":{\"id\":789}}")
                        .withFixedDelay(1000)
                )
        );

        // the run this attempt's own POST created genuinely failed
        stubFor(
            get(urlPathEqualTo("/api/v2/accounts/123/runs/"))
                .inScenario("adopt-on-timeout-failed")
                .whenScenarioStateIs("run created")
                .withQueryParam("job_definition_id", equalTo("456"))
                .willReturn(okJson("{\"data\":[{\"id\":789,\"status\":20,\"trigger\":{\"cause\":\"Triggered by Kestra. " + tag + "\"}}]}"))
        );

        TriggerRun.Output output = task.run(runContext);

        // adopted the failed run (wait=false just reports its id; wait=true would surface its failure)
        assertThat(output.getRunId(), is(789L));
        verify(1, postRequestedFor(urlEqualTo("/api/v2/accounts/123/jobs/456/run/")));
    }

    @Test
    void testFailsWhenAmbiguousTimeoutFindsNoMatchingRun() throws Exception {
        HttpConfiguration shortTimeout = HttpConfiguration.builder()
            .timeout(TimeoutConfiguration.builder().readIdleTimeout(Property.ofValue(Duration.ofMillis(300))).build())
            .build();

        TriggerRun task = TriggerRun.builder()
            .id(IdUtils.create())
            .type(TriggerRun.class.getName())
            .accountId(Property.ofValue("123"))
            .jobId(Property.ofValue("456"))
            .token(Property.ofValue("my-token"))
            .baseUrl(Property.ofValue("http://localhost:28181"))
            .reattach(Property.ofValue(true))
            .wait(Property.ofValue(false))
            .options(shortTimeout)
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());

        // the lookup never finds a run carrying this taskrun's tag: the POST genuinely never created one
        stubFor(
            get(urlPathEqualTo("/api/v2/accounts/123/runs/"))
                .withQueryParam("job_definition_id", equalTo("456"))
                .willReturn(okJson("{\"data\":[]}"))
        );

        stubFor(
            post(urlEqualTo("/api/v2/accounts/123/jobs/456/run/"))
                .willReturn(
                    aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody("{\"data\":{\"id\":789}}")
                        .withFixedDelay(1000)
                )
        );

        assertThatThrownBy(() -> task.run(runContext))
            .hasRootCauseInstanceOf(java.net.SocketTimeoutException.class);
        verify(1, postRequestedFor(urlEqualTo("/api/v2/accounts/123/jobs/456/run/")));
    }

    @Test
    void testConfirmLookupFailureDoesNotMaskOriginalFailure() throws Exception {
        // The trigger POST times out (ambiguous), and the follow-up confirm-and-adopt lookup itself
        // fails (500). The task must fail loud with the ORIGINAL timeout, never the lookup failure,
        // and it must not have triggered a duplicate run.
        HttpConfiguration shortTimeout = HttpConfiguration.builder()
            .timeout(TimeoutConfiguration.builder().readIdleTimeout(Property.ofValue(Duration.ofMillis(300))).build())
            .build();

        TriggerRun task = TriggerRun.builder()
            .id(IdUtils.create())
            .type(TriggerRun.class.getName())
            .accountId(Property.ofValue("123"))
            .jobId(Property.ofValue("456"))
            .token(Property.ofValue("my-token"))
            .baseUrl(Property.ofValue("http://localhost:28181"))
            .reattach(Property.ofValue(true))
            .wait(Property.ofValue(false))
            .options(shortTimeout)
            .maxRetries(Property.ofValue(1))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());

        // no run started by this taskrun yet: the start-of-run reattach lookup finds nothing, so the trigger fires
        stubFor(
            get(urlPathEqualTo("/api/v2/accounts/123/runs/"))
                .inScenario("confirm-lookup-fails")
                .whenScenarioStateIs(STARTED)
                .withQueryParam("job_definition_id", equalTo("456"))
                .willReturn(okJson("{\"data\":[]}"))
                .willSetStateTo("trigger sent")
        );

        // the trigger POST reaches dbt Cloud (which creates the run) but its response is lost on the wire
        stubFor(
            post(urlEqualTo("/api/v2/accounts/123/jobs/456/run/"))
                .willReturn(
                    aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody("{\"data\":{\"id\":789}}")
                        .withFixedDelay(1000)
                )
        );

        // the confirm-and-adopt lookup after the timeout itself fails: it must never mask the original timeout
        stubFor(
            get(urlPathEqualTo("/api/v2/accounts/123/runs/"))
                .inScenario("confirm-lookup-fails")
                .whenScenarioStateIs("trigger sent")
                .withQueryParam("job_definition_id", equalTo("456"))
                .willReturn(aResponse().withStatus(500).withHeader("Content-Type", "application/json"))
        );

        assertThatThrownBy(() -> task.run(runContext))
            .hasRootCauseInstanceOf(java.net.SocketTimeoutException.class);
        verify(1, postRequestedFor(urlEqualTo("/api/v2/accounts/123/jobs/456/run/")));
    }

    @Test
    void testReattachAdoptsAlreadyFinishedRun() throws Exception {
        TriggerRun task = TriggerRun.builder()
            .id(IdUtils.create())
            .type(TriggerRun.class.getName())
            .accountId(Property.ofValue("123"))
            .jobId(Property.ofValue("456"))
            .token(Property.ofValue("my-token"))
            .baseUrl(Property.ofValue("http://localhost:28181"))
            .reattach(Property.ofValue(true))
            .wait(Property.ofValue(false))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());
        String tag = "[taskrun:" + runContext.taskRunInfo().taskRunId() + "]";

        // this taskrun's run already completed (status 10 = success) before this re-run started
        stubFor(
            get(urlPathEqualTo("/api/v2/accounts/123/runs/"))
                .withQueryParam("job_definition_id", equalTo("456"))
                .withQueryParam("include_related", matching(".*trigger.*"))
                .willReturn(okJson("{\"data\":[{\"id\":789,\"status\":10,\"trigger\":{\"cause\":\"Triggered by Kestra. " + tag + "\"}}]}"))
        );

        TriggerRun.Output output = task.run(runContext);

        assertThat(output.getRunId(), is(789L));
        verify(0, postRequestedFor(urlEqualTo("/api/v2/accounts/123/jobs/456/run/")));
    }

    @Test
    void testReattachRetriggersOnGenuinelyFailedRunAtStartOfRun() throws Exception {
        // Regression test: a run that this taskrun started genuinely FAILED (status 20). A task-level
        // `retry` (or a manual "Restart from failed task") re-runs this taskrun id, hits the start-of-run
        // reattach check, and must trigger a fresh run rather than silently re-adopting the same failure.
        TriggerRun task = TriggerRun.builder()
            .id(IdUtils.create())
            .type(TriggerRun.class.getName())
            .accountId(Property.ofValue("123"))
            .jobId(Property.ofValue("456"))
            .token(Property.ofValue("my-token"))
            .baseUrl(Property.ofValue("http://localhost:28181"))
            .reattach(Property.ofValue(true))
            .wait(Property.ofValue(true))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());
        String tag = "[taskrun:" + runContext.taskRunInfo().taskRunId() + "]";

        stubFor(
            get(urlPathEqualTo("/api/v2/accounts/123/runs/"))
                .withQueryParam("job_definition_id", equalTo("456"))
                .withQueryParam("include_related", matching(".*trigger.*"))
                .willReturn(okJson("{\"data\":[{\"id\":789,\"status\":20,\"trigger\":{\"cause\":\"Triggered by Kestra. " + tag + "\"}}]}"))
        );

        stubFor(
            post(urlEqualTo("/api/v2/accounts/123/jobs/456/run/"))
                .willReturn(okJson("{\"data\":{\"id\":790}}"))
        );

        stubFor(
            get(urlPathEqualTo("/api/v2/accounts/123/runs/790/"))
                .withQueryParam("include_related", matching(".*run_steps.*"))
                .willReturn(okJson("""
                        {
                          "data": {
                            "id": 790,
                            "status_humanized": "Success",
                            "duration_humanized": "1m",
                            "run_steps": []
                          }
                        }
                    """))
        );
        stubFor(
            get(urlEqualTo("/api/v2/accounts/123/runs/790/artifacts/run_results.json"))
                .willReturn(aResponse().withStatus(404))
        );
        stubFor(
            get(urlEqualTo("/api/v2/accounts/123/runs/790/artifacts/manifest.json"))
                .willReturn(aResponse().withStatus(404))
        );

        TriggerRun.Output output = task.run(runContext);

        assertThat(output.getRunId(), is(790L));
        // did NOT adopt the failed run 789, triggered a fresh one instead
        verify(1, postRequestedFor(urlEqualTo("/api/v2/accounts/123/jobs/456/run/")));
    }

    @Test
    void testReattachOffSkipsLookupAndPropagatesOriginalTimeoutOnPostFailure() throws Exception {
        // reattach = false: no lookup should ever happen, not at start-of-run and not on an ambiguous
        // POST failure. The original timeout must propagate untouched and no duplicate POST is sent.
        HttpConfiguration shortTimeout = HttpConfiguration.builder()
            .timeout(TimeoutConfiguration.builder().readIdleTimeout(Property.ofValue(Duration.ofMillis(300))).build())
            .build();

        TriggerRun task = TriggerRun.builder()
            .id(IdUtils.create())
            .type(TriggerRun.class.getName())
            .accountId(Property.ofValue("123"))
            .jobId(Property.ofValue("456"))
            .token(Property.ofValue("my-token"))
            .baseUrl(Property.ofValue("http://localhost:28181"))
            .reattach(Property.ofValue(false))
            .wait(Property.ofValue(false))
            .options(shortTimeout)
            .maxRetries(Property.ofValue(1))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());

        stubFor(
            post(urlEqualTo("/api/v2/accounts/123/jobs/456/run/"))
                .willReturn(
                    aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody("{\"data\":{\"id\":789}}")
                        .withFixedDelay(1000)
                )
        );

        assertThatThrownBy(() -> task.run(runContext))
            .hasRootCauseInstanceOf(java.net.SocketTimeoutException.class);
        verify(0, getRequestedFor(urlPathEqualTo("/api/v2/accounts/123/runs/")));
        verify(1, postRequestedFor(urlEqualTo("/api/v2/accounts/123/jobs/456/run/")));
    }

    @Test
    void testDoesNotAdoptStaleRunFromPriorAttempt() throws Exception {
        // Regression: a prior attempt's run (same taskrun id, tag reused) failed with status 20 (Error).
        // This attempt's own POST times out, and every confirm lookup afterward still only turns up that
        // same stale run, never a newer id. The original timeout must surface, never the stale run's
        // Error outcome, and its id must be rejected as the baseline throughout the confirm loop.
        HttpConfiguration shortTimeout = HttpConfiguration.builder()
            .timeout(TimeoutConfiguration.builder().readIdleTimeout(Property.ofValue(Duration.ofMillis(300))).build())
            .build();

        TriggerRun task = TriggerRun.builder()
            .id(IdUtils.create())
            .type(TriggerRun.class.getName())
            .accountId(Property.ofValue("123"))
            .jobId(Property.ofValue("456"))
            .token(Property.ofValue("my-token"))
            .baseUrl(Property.ofValue("http://localhost:28181"))
            .reattach(Property.ofValue(true))
            .wait(Property.ofValue(false))
            .options(shortTimeout)
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());
        String tag = "[taskrun:" + runContext.taskRunInfo().taskRunId() + "]";

        // every lookup (the start-of-run check AND every confirm retry) only ever turns up the stale,
        // already-failed run left over from a prior attempt
        stubFor(
            get(urlPathEqualTo("/api/v2/accounts/123/runs/"))
                .withQueryParam("job_definition_id", equalTo("456"))
                .willReturn(okJson("{\"data\":[{\"id\":501,\"status\":20,\"trigger\":{\"cause\":\"Triggered by Kestra. " + tag + "\"}}]}"))
        );

        // this attempt's own POST reaches dbt Cloud (creating run 502) but the response is lost on the wire
        stubFor(
            post(urlEqualTo("/api/v2/accounts/123/jobs/456/run/"))
                .willReturn(
                    aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody("{\"data\":{\"id\":502}}")
                        .withFixedDelay(1000)
                )
        );

        assertThatThrownBy(() -> task.run(runContext))
            .hasRootCauseInstanceOf(java.net.SocketTimeoutException.class);
        verify(1, postRequestedFor(urlEqualTo("/api/v2/accounts/123/jobs/456/run/")));
    }

    @Test
    void testAdoptsLaggingNewRunNotStalePriorAttempt() throws Exception {
        // Prior attempt's run #501 (Error) is the baseline. This attempt's POST times out and creates
        // run #502, but the confirm lookup lags behind: it first still only shows #501 before #502
        // appears. Only a run whose id is strictly greater than the baseline may be adopted, so the
        // stale #501 is rejected even though it matches the same tag, and the loop keeps waiting until
        // the genuinely new #502 shows up.
        HttpConfiguration shortTimeout = HttpConfiguration.builder()
            .timeout(TimeoutConfiguration.builder().readIdleTimeout(Property.ofValue(Duration.ofMillis(300))).build())
            .build();

        TriggerRun task = TriggerRun.builder()
            .id(IdUtils.create())
            .type(TriggerRun.class.getName())
            .accountId(Property.ofValue("123"))
            .jobId(Property.ofValue("456"))
            .token(Property.ofValue("my-token"))
            .baseUrl(Property.ofValue("http://localhost:28181"))
            .reattach(Property.ofValue(true))
            .wait(Property.ofValue(false))
            .options(shortTimeout)
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());
        String tag = "[taskrun:" + runContext.taskRunInfo().taskRunId() + "]";

        // hit 1 (start-of-run check): only the stale #501 exists yet
        stubFor(
            get(urlPathEqualTo("/api/v2/accounts/123/runs/"))
                .inScenario("lagging-new-run")
                .whenScenarioStateIs(STARTED)
                .withQueryParam("job_definition_id", equalTo("456"))
                .willReturn(okJson("{\"data\":[{\"id\":501,\"status\":20,\"trigger\":{\"cause\":\"Triggered by Kestra. " + tag + "\"}}]}"))
                .willSetStateTo("confirm attempt 1")
        );
        // hit 2 (first confirm retry): still just #501, stale, must be rejected
        stubFor(
            get(urlPathEqualTo("/api/v2/accounts/123/runs/"))
                .inScenario("lagging-new-run")
                .whenScenarioStateIs("confirm attempt 1")
                .withQueryParam("job_definition_id", equalTo("456"))
                .willReturn(okJson("{\"data\":[{\"id\":501,\"status\":20,\"trigger\":{\"cause\":\"Triggered by Kestra. " + tag + "\"}}]}"))
                .willSetStateTo("new run appeared")
        );
        // hit 3 (second confirm retry): the genuinely new, succeeded run #502 has now caught up
        stubFor(
            get(urlPathEqualTo("/api/v2/accounts/123/runs/"))
                .inScenario("lagging-new-run")
                .whenScenarioStateIs("new run appeared")
                .withQueryParam("job_definition_id", equalTo("456"))
                .willReturn(okJson("{\"data\":[{\"id\":502,\"status\":10,\"trigger\":{\"cause\":\"Triggered by Kestra. " + tag + "\"}}]}"))
        );

        stubFor(
            post(urlEqualTo("/api/v2/accounts/123/jobs/456/run/"))
                .willReturn(
                    aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody("{\"data\":{\"id\":502}}")
                        .withFixedDelay(1000)
                )
        );

        TriggerRun.Output output = task.run(runContext);

        // adopted the genuinely new #502, not the stale #501 baseline
        assertThat(output.getRunId(), is(502L));
        verify(1, postRequestedFor(urlEqualTo("/api/v2/accounts/123/jobs/456/run/")));
    }

    @Test
    void testAdoptsRunAfterGatewayError() throws Exception {
        // A 504 gateway timeout on the trigger POST is ambiguous: the proxy gave up, but dbt Cloud behind
        // it may already have received the request and created the run. With reattach on it must NOT be
        // blindly retried (that would duplicate); it is confirmed and the created run is adopted.
        TriggerRun task = TriggerRun.builder()
            .id(IdUtils.create())
            .type(TriggerRun.class.getName())
            .accountId(Property.ofValue("123"))
            .jobId(Property.ofValue("456"))
            .token(Property.ofValue("my-token"))
            .baseUrl(Property.ofValue("http://localhost:28181"))
            .reattach(Property.ofValue(true))
            .wait(Property.ofValue(false))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());
        String tag = "[taskrun:" + runContext.taskRunInfo().taskRunId() + "]";

        // start-of-run lookup finds nothing, then the confirm lookup turns up the run the POST created
        stubFor(
            get(urlPathEqualTo("/api/v2/accounts/123/runs/"))
                .inScenario("adopt-on-gateway")
                .whenScenarioStateIs(STARTED)
                .withQueryParam("job_definition_id", equalTo("456"))
                .willReturn(okJson("{\"data\":[]}"))
                .willSetStateTo("run created")
        );
        stubFor(
            get(urlPathEqualTo("/api/v2/accounts/123/runs/"))
                .inScenario("adopt-on-gateway")
                .whenScenarioStateIs("run created")
                .withQueryParam("job_definition_id", equalTo("456"))
                .willReturn(okJson("{\"data\":[{\"id\":789,\"status\":10,\"trigger\":{\"cause\":\"Triggered by Kestra. " + tag + "\"}}]}"))
        );

        // the trigger POST returns a 504 (the proxy timed out) even though dbt created run 789 behind it
        stubFor(
            post(urlEqualTo("/api/v2/accounts/123/jobs/456/run/"))
                .willReturn(aResponse().withStatus(504))
        );

        TriggerRun.Output output = task.run(runContext);

        assertThat(output.getRunId(), is(789L));
        // a single POST: the ambiguous 504 was confirmed-and-adopted, never blindly retried into a duplicate
        verify(1, postRequestedFor(urlEqualTo("/api/v2/accounts/123/jobs/456/run/")));
    }

    @Test
    void testGatewayErrorCreatingNoRunIsNotBlindlyRetried() throws Exception {
        // Same 504, but it created no run. The task must fail with the gateway error and, crucially, must
        // NOT have re-sent the POST: with reattach on the ambiguous 504 is routed to confirm-and-adopt, not
        // to the generic HTTP-layer retry that would otherwise re-POST and risk a duplicate.
        TriggerRun task = TriggerRun.builder()
            .id(IdUtils.create())
            .type(TriggerRun.class.getName())
            .accountId(Property.ofValue("123"))
            .jobId(Property.ofValue("456"))
            .token(Property.ofValue("my-token"))
            .baseUrl(Property.ofValue("http://localhost:28181"))
            .reattach(Property.ofValue(true))
            .wait(Property.ofValue(false))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());

        // no run carrying this taskrun's tag ever appears: the 504 genuinely created nothing
        stubFor(
            get(urlPathEqualTo("/api/v2/accounts/123/runs/"))
                .withQueryParam("job_definition_id", equalTo("456"))
                .willReturn(okJson("{\"data\":[]}"))
        );
        stubFor(
            post(urlEqualTo("/api/v2/accounts/123/jobs/456/run/"))
                .willReturn(aResponse().withStatus(504))
        );

        // the original 504 surfaces unmasked (not the confirm lookup's own give-up), and only one POST was sent
        assertThatThrownBy(() -> task.run(runContext)).isInstanceOf(HttpClientResponseException.class);
        verify(1, postRequestedFor(urlEqualTo("/api/v2/accounts/123/jobs/456/run/")));
    }
}
