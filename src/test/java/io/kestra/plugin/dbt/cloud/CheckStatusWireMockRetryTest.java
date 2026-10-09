package io.kestra.plugin.dbt.cloud;

import java.time.Duration;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import com.github.tomakehurst.wiremock.stubbing.Scenario;

import io.kestra.core.http.client.configurations.HttpConfiguration;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.retrys.Constant;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;

import jakarta.inject.Inject;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Exercises Core's real retry loop for CheckStatus against a mocked server.
 */
@KestraTest
@WireMockTest(httpPort = 28185)
class CheckStatusWireMockRetryTest {
    private static final String RUN_PATH = "/api/v2/accounts/123/runs/9999/";

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void retriesAServerErrorThenSucceeds() throws Exception {
        stubFor(
            get(urlPathEqualTo(RUN_PATH))
                .inScenario("flaky-read")
                .whenScenarioStateIs(Scenario.STARTED)
                .willReturn(aResponse().withStatus(503))
                .willSetStateTo("recovered")
        );
        stubFor(
            get(urlPathEqualTo(RUN_PATH))
                .inScenario("flaky-read")
                .whenScenarioStateIs("recovered")
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

        var output = checkStatus(fastRetry(3), Duration.ofSeconds(30)).run(runContextFactory.of(Map.of()));

        assertThat(output.getRunId(), is(9999L));
        // One 503, then the poll read, then the final debug read.
        verify(3, getRequestedFor(urlPathEqualTo(RUN_PATH)));
    }

    @Test
    void pollsAgainAfterAnEmptyResponseBody() throws Exception {
        stubFor(
            get(urlPathEqualTo(RUN_PATH))
                .inScenario("empty-read")
                .whenScenarioStateIs(Scenario.STARTED)
                .willReturn(aResponse().withStatus(200))
                .willSetStateTo("recovered")
        );
        stubFor(
            get(urlPathEqualTo(RUN_PATH))
                .inScenario("empty-read")
                .whenScenarioStateIs("recovered")
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

        var output = checkStatus(fastRetry(3), Duration.ofSeconds(30)).run(runContextFactory.of(Map.of()));

        assertThat(output.getRunId(), is(9999L));
        // Empty-body parsing fails outside Core's HTTP retry loop, so CheckStatus tries again next poll.
        verify(3, getRequestedFor(urlPathEqualTo(RUN_PATH)));
    }

    @Test
    void stopsRetryingOncePerRequestAttemptsAreExhausted() {
        stubFor(get(urlPathEqualTo(RUN_PATH)).willReturn(aResponse().withStatus(503)));

        // Keep maxDuration below pollFrequency so only one polling cycle is attempted.
        var task = checkStatus(fastRetry(3), Duration.ofMillis(100));
        assertThrows(Exception.class, () -> task.run(runContextFactory.of(Map.of())));

        // Core exhausted the configured three attempts for this request before CheckStatus timed out.
        var requests = findAll(getRequestedFor(urlPathEqualTo(RUN_PATH))).size();
        assertThat(requests, is(3));
    }

    private HttpConfiguration fastRetry(int maxAttempts) {
        return HttpConfiguration.builder()
            .retry(Constant.builder().interval(Duration.ofMillis(10)).maxAttempts(maxAttempts).build())
            .build();
    }

    private CheckStatus checkStatus(HttpConfiguration options, Duration maxDuration) {
        return CheckStatus.builder()
            .id(IdUtils.create())
            .type(CheckStatus.class.getName())
            .runId(Property.ofValue("9999"))
            .accountId(Property.ofValue("123"))
            .token(Property.ofValue("my-token"))
            .baseUrl(Property.ofValue("http://localhost:28185"))
            .pollFrequency(Property.ofValue(Duration.ofMillis(200)))
            .maxDuration(Property.ofValue(maxDuration))
            .options(options)
            .build();
    }
}