package io.kestra.plugin.dbt.cloud;

import java.util.Map;

import org.junit.jupiter.api.Test;

import com.github.tomakehurst.wiremock.junit5.WireMockTest;

import io.kestra.core.http.client.HttpClientResponseException;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;

import jakarta.inject.Inject;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
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
                .withQueryParam("status__in", equalTo("[1,2,3]"))
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

        assertThatThrownBy(() -> task.run(runContext)).isInstanceOf(Exception.class);
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
}
