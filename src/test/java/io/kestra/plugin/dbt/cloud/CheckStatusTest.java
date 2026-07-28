package io.kestra.plugin.dbt.cloud;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import com.github.tomakehurst.wiremock.stubbing.Scenario;
import io.kestra.core.http.client.HttpClientResponseException;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.junit.annotations.KestraTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
@WireMockTest(httpPort = 8089)
class CheckStatusTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void run() throws Exception {

        stubFor(post(urlEqualTo("/api/v2/accounts/123/jobs/456/run/"))
            .willReturn(okJson("""
                { "data": { "id": 9999 } }
            """)));

        stubFor(get(urlMatching("/api/v2/accounts/123/runs/9999/.*"))
            .willReturn(okJson("""
                {
                  "data": {
                    "id": 9999,
                    "status_humanized": "Success",
                    "duration_humanized": "0s",
                    "run_steps": []
                  }
                }
            """)));

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
     * A transient failure while polling run status (e.g. a dbt Cloud 500 gateway page) must not fail
     * the task: the run is still healthy. The loop logs it and polls again, succeeding on the next
     * status read.
     */
    @Test
    void shouldTolerateTransientStatusReadFailure() throws Exception {
        stubFor(get(urlMatching("/api/v2/accounts/123/runs/9090/\\?.*"))
            .inScenario("transient-then-success")
            .whenScenarioStateIs(Scenario.STARTED)
            .willReturn(aResponse()
                .withStatus(500)
                .withHeader("Content-Type", "text/html")
                .withBody("<html><title>Uh oh! | dbt</title></html>"))
            .willSetStateTo("recovered"));

        stubFor(get(urlMatching("/api/v2/accounts/123/runs/9090/\\?.*"))
            .inScenario("transient-then-success")
            .whenScenarioStateIs("recovered")
            .willReturn(okJson("""
                {
                  "data": {
                    "id": 9090,
                    "status_humanized": "Success",
                    "duration_humanized": "1s",
                    "run_steps": []
                  }
                }
            """)));

        stubFor(get(urlMatching("/api/v2/accounts/123/runs/9090/artifacts/.*"))
            .willReturn(okJson("{}")));

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
        stubFor(get(urlMatching("/api/v2/accounts/123/runs/9091/\\?.*"))
            .willReturn(aResponse().withStatus(404).withBody("Not Found")));

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
}
