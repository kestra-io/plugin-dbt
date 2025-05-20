package io.kestra.plugin.dbt.cloud;

import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.runners.RunContext;
import io.kestra.core.utils.IdUtils;

import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;

import com.github.tomakehurst.wiremock.junit5.WireMockTest;

import java.util.Map;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.hamcrest.Matchers.*;

@KestraTest
@WireMockTest(httpPort = 28181)
class MockTriggerRunTest {

    @Inject
    RunContextFactory runContextFactory;

    @Test
    void testTriggerRun() throws Exception {

        stubFor(post(urlEqualTo("/api/v2/accounts/123/jobs/456/run/"))
            .willReturn(aResponse()
                .withStatus(200)
                .withHeader("Content-Type", "application/json")
                .withBody("{\"data\":{\"id\":789}}")));

        TriggerRun task = TriggerRun.builder()
            .id(IdUtils.create())
            .type(TriggerRun.class.getName())
            .accountId(Property.of("123"))
            .jobId(Property.of("456"))
            .token(Property.of("my-token"))
            .baseUrl(Property.of("http://localhost:28181"))
            .wait(Property.of(false))
            .build();

        RunContext runContext = runContextFactory.of(Map.of());
        TriggerRun.Output output = task.run(runContext);

        assertThat(output, is(notNullValue()));
        assertThat(output.getRunId(), is(789L));
    }

    @Test
    void testTriggerRunWithWait() throws Exception {

        stubFor(post(urlEqualTo("/api/v2/accounts/123/jobs/456/run/"))
            .willReturn(okJson("{\"data\":{\"id\":789}}")));

        stubFor(get(urlPathEqualTo("/api/v2/accounts/123/runs/789/"))
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
        """)));

        // stub for run result artifacts
        stubFor(get(urlEqualTo("/api/v2/accounts/123/runs/789/artifacts/run_results.json"))
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
        """)));

        // stub for run manifest artifacts
        stubFor(get(urlEqualTo("/api/v2/accounts/123/runs/789/artifacts/manifest.json"))
            .willReturn(okJson("{\"nodes\": {}}")));

        TriggerRun task = TriggerRun.builder()
            .id(IdUtils.create())
            .type(TriggerRun.class.getName())
            .accountId(Property.of("123"))
            .jobId(Property.of("456"))
            .token(Property.of("demo"))
            .parseRunResults(Property.of(true))
            .baseUrl(Property.of("http://localhost:28181"))
            .wait(Property.of(true))
            .build();

        RunContext runContext = runContextFactory.of(Map.of(
            "flow", Map.of(
                "id", "my-flow",
                "namespace", "my.namespace"
            ),
            "execution", Map.of(
                "id", "exec-123"
            ),
            "taskrun", Map.of(
                "id", "taskrun-123"
            )
        ));
        TriggerRun.Output output = task.run(runContext);

        assertThat(output, is(notNullValue()));
        assertThat(output.getRunId(), is(789L));
        assertThat(output.getRunResults().toString(), containsString("kestra://"));
        assertThat(output.getManifest(), is(notNullValue()));
    }
}
