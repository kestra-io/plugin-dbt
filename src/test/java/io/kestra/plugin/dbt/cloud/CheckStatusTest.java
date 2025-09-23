package io.kestra.plugin.dbt.cloud;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
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
}
