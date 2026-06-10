package io.kestra.plugin.dbt.cloud;

import java.time.Duration;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.github.tomakehurst.wiremock.junit5.WireMockTest;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;

import jakarta.inject.Inject;

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
}
