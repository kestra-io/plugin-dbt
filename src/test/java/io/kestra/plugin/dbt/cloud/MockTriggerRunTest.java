package io.kestra.plugin.dbt.cloud;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.Test;

import com.github.tomakehurst.wiremock.junit5.WireMockTest;

import io.kestra.core.http.client.HttpClientResponseException;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.kv.KVStore;
import io.kestra.core.storages.kv.KVValueAndMetadata;
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

    private static void stubSuccessfulRunAndArtifacts() {
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

        stubFor(
            get(urlEqualTo("/api/v2/accounts/123/runs/789/artifacts/manifest.json"))
                .willReturn(okJson("{\"nodes\": {}}"))
        );
    }

    @Test
    void resumeSkipsTrigger() throws Exception {
        TriggerRun task = TriggerRun.builder()
            .id(IdUtils.create())
            .type(TriggerRun.class.getName())
            .accountId(Property.ofValue("123"))
            .jobId(Property.ofValue("456"))
            .token(Property.ofValue("demo"))
            .baseUrl(Property.ofValue("http://localhost:28181"))
            .wait(Property.ofValue(true))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());
        String key = "dbt_cloud_resume_" + runContext.taskRunInfo().taskRunId();
        KVStore kv = runContext.namespaceKv(runContext.flowInfo().namespace());
        kv.put(key, new KVValueAndMetadata(null, "789"));

        stubSuccessfulRunAndArtifacts();

        TriggerRun.Output output = task.run(runContext);

        verify(0, postRequestedFor(urlEqualTo("/api/v2/accounts/123/jobs/456/run/")));
        assertThat(output.getRunId(), is(789L));
        assertThat(kv.getValue(key).isEmpty(), is(true));
    }

    @Test
    void happyPathPersistsThenCleansUp() throws Exception {
        stubFor(
            post(urlEqualTo("/api/v2/accounts/123/jobs/456/run/"))
                .willReturn(okJson("{\"data\":{\"id\":789}}"))
        );
        stubSuccessfulRunAndArtifacts();

        TriggerRun task = TriggerRun.builder()
            .id(IdUtils.create())
            .type(TriggerRun.class.getName())
            .accountId(Property.ofValue("123"))
            .jobId(Property.ofValue("456"))
            .token(Property.ofValue("demo"))
            .baseUrl(Property.ofValue("http://localhost:28181"))
            .wait(Property.ofValue(true))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());
        String key = "dbt_cloud_resume_" + runContext.taskRunInfo().taskRunId();
        KVStore kv = runContext.namespaceKv(runContext.flowInfo().namespace());

        TriggerRun.Output output = task.run(runContext);

        verify(1, postRequestedFor(urlEqualTo("/api/v2/accounts/123/jobs/456/run/")));
        assertThat(output.getRunId(), is(789L));
        assertThat(kv.getValue(key).isEmpty(), is(true));
    }

    @Test
    void optOutStillTriggers() throws Exception {
        stubFor(
            post(urlEqualTo("/api/v2/accounts/123/jobs/456/run/"))
                .willReturn(okJson("{\"data\":{\"id\":789}}"))
        );
        stubSuccessfulRunAndArtifacts();

        TriggerRun task = TriggerRun.builder()
            .id(IdUtils.create())
            .type(TriggerRun.class.getName())
            .accountId(Property.ofValue("123"))
            .jobId(Property.ofValue("456"))
            .token(Property.ofValue("demo"))
            .baseUrl(Property.ofValue("http://localhost:28181"))
            .wait(Property.ofValue(true))
            .resumeOnRestart(Property.ofValue(false))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());
        String key = "dbt_cloud_resume_" + runContext.taskRunInfo().taskRunId();
        KVStore kv = runContext.namespaceKv(runContext.flowInfo().namespace());
        kv.put(key, new KVValueAndMetadata(null, "789"));

        task.run(runContext);

        verify(1, postRequestedFor(urlEqualTo("/api/v2/accounts/123/jobs/456/run/")));
    }

    @Test
    void keepsKeyOnTimeout() throws Exception {
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
                            "status_humanized": "Running",
                            "duration_humanized": "1m",
                            "run_steps": []
                          }
                        }
                    """))
        );

        TriggerRun task = TriggerRun.builder()
            .id(IdUtils.create())
            .type(TriggerRun.class.getName())
            .accountId(Property.ofValue("123"))
            .jobId(Property.ofValue("456"))
            .token(Property.ofValue("demo"))
            .baseUrl(Property.ofValue("http://localhost:28181"))
            .wait(Property.ofValue(true))
            .pollFrequency(Property.ofValue(Duration.ofMillis(100)))
            .maxDuration(Property.ofValue(Duration.ofSeconds(1)))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());
        String key = "dbt_cloud_resume_" + runContext.taskRunInfo().taskRunId();
        KVStore kv = runContext.namespaceKv(runContext.flowInfo().namespace());

        assertThatThrownBy(() -> task.run(runContext))
            .isInstanceOf(TimeoutException.class);

        assertThat(kv.getValue(key).isPresent(), is(true));
    }

    @Test
    void terminalFailureDeletesToken() throws Exception {
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
                            "status": 20,
                            "status_humanized": "Error",
                            "duration_humanized": "1m",
                            "run_steps": []
                          }
                        }
                    """))
        );

        TriggerRun task = TriggerRun.builder()
            .id(IdUtils.create())
            .type(TriggerRun.class.getName())
            .accountId(Property.ofValue("123"))
            .jobId(Property.ofValue("456"))
            .token(Property.ofValue("demo"))
            .baseUrl(Property.ofValue("http://localhost:28181"))
            .wait(Property.ofValue(true))
            .resumeOnRestart(Property.ofValue(true))
            .pollFrequency(Property.ofValue(Duration.ofMillis(100)))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());
        String key = "dbt_cloud_resume_" + runContext.taskRunInfo().taskRunId();
        KVStore kv = runContext.namespaceKv(runContext.flowInfo().namespace());

        assertThatThrownBy(() -> task.run(runContext))
            .hasMessageContaining("Failed run with status")
            .hasMessageContaining("Error");

        assertThat(kv.getValue(key).isEmpty(), is(true));
    }
}
