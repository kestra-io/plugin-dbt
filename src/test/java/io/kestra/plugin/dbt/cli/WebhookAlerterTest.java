package io.kestra.plugin.dbt.cli;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.github.tomakehurst.wiremock.junit5.WireMockTest;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;

import jakarta.inject.Inject;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.lessThan;

@KestraTest
@WireMockTest(httpPort = 28282)
class WebhookAlerterTest {
    @Inject
    private RunContextFactory runContextFactory;

    private static final String WEBHOOK_URL = "http://localhost:28282/hooks/alert";

    @Test
    void warnAlert_shouldSendOnePost() {
        stubFor(post(urlEqualTo("/hooks/alert")).willReturn(ok()));

        try (var alerter = new WebhookAlerter(mockRunContext(), WEBHOOK_URL, DbtCLI.AlertLevel.WARN)) {
            alerter.maybeAlert("warn", "model.my_project.my_model", "DeprecationWarning", "Deprecated feature used", "2024-01-01T00:00:00Z");
        }

        verify(
            1,
            postRequestedFor(urlEqualTo("/hooks/alert"))
                .withRequestBody(matchingJsonPath("$.level", equalTo("warn")))
                .withRequestBody(matchingJsonPath("$.node", equalTo("model.my_project.my_model")))
                .withRequestBody(matchingJsonPath("$.type", equalTo("DeprecationWarning")))
                .withRequestBody(matchingJsonPath("$.msg", equalTo("Deprecated feature used")))
                .withRequestBody(matchingJsonPath("$.text", containing("model.my_project.my_model")))
        );
    }

    @Test
    void duplicateNode_shouldSendOnlyOnePost() {
        stubFor(post(urlEqualTo("/hooks/alert")).willReturn(ok()));

        try (var alerter = new WebhookAlerter(mockRunContext(), WEBHOOK_URL, DbtCLI.AlertLevel.WARN)) {
            alerter.maybeAlert("warn", "model.my_project.my_model", "DeprecationWarning", "Deprecated feature used", "2024-01-01T00:00:00Z");
            alerter.maybeAlert("warn", "model.my_project.my_model", "DeprecationWarning", "Deprecated feature used again", "2024-01-01T00:00:01Z");
        }

        verify(1, postRequestedFor(urlEqualTo("/hooks/alert")));
    }

    @Test
    void capExceeded_shouldSuppressAndSendOneSummary() {
        stubFor(post(urlEqualTo("/hooks/alert")).willReturn(ok()));

        try (var alerter = new WebhookAlerter(mockRunContext(), WEBHOOK_URL, DbtCLI.AlertLevel.WARN)) {
            for (int i = 0; i < WebhookAlerter.MAX_ALERTS + 1; i++) {
                alerter.maybeAlert("warn", "model.my_project.model_" + i, "Warning", "msg " + i, "2024-01-01T00:00:00Z");
            }
        }

        verify(WebhookAlerter.MAX_ALERTS, postRequestedFor(urlEqualTo("/hooks/alert")).withRequestBody(matchingJsonPath("$.node")));
        verify(1, postRequestedFor(urlEqualTo("/hooks/alert")).withRequestBody(matchingJsonPath("$.text", containing("suppressed"))));
    }

    @Test
    void errorMinLevel_shouldIgnoreWarn() {
        stubFor(post(urlEqualTo("/hooks/alert")).willReturn(ok()));

        try (var alerter = new WebhookAlerter(mockRunContext(), WEBHOOK_URL, DbtCLI.AlertLevel.ERROR)) {
            alerter.maybeAlert("warn", "model.my_project.my_model", "Warning", "Just a warning", "2024-01-01T00:00:00Z");
        }

        verify(0, postRequestedFor(urlEqualTo("/hooks/alert")));
    }

    @Test
    void errorMinLevel_shouldStillAlertOnError() {
        stubFor(post(urlEqualTo("/hooks/alert")).willReturn(ok()));

        try (var alerter = new WebhookAlerter(mockRunContext(), WEBHOOK_URL, DbtCLI.AlertLevel.ERROR)) {
            alerter.maybeAlert("error", "model.my_project.my_model", "CompilationError", "Model failed to compile", "2024-01-01T00:00:00Z");
        }

        verify(1, postRequestedFor(urlEqualTo("/hooks/alert")).withRequestBody(matchingJsonPath("$.level", equalTo("error"))));
    }

    @Test
    void serverError_shouldNotThrowAndCloseWithinBound() {
        stubFor(post(urlEqualTo("/hooks/alert")).willReturn(aResponse().withStatus(500)));

        var alerter = new WebhookAlerter(mockRunContext(), WEBHOOK_URL, DbtCLI.AlertLevel.WARN);
        alerter.maybeAlert("warn", "model.my_project.my_model", "Warning", "boom", "2024-01-01T00:00:00Z");

        long start = System.currentTimeMillis();
        alerter.close();
        long elapsed = System.currentTimeMillis() - start;

        assertThat(elapsed, lessThan(7000L));
    }

    @Test
    void timeout_shouldNotThrowAndCloseWithinBound() {
        stubFor(post(urlEqualTo("/hooks/alert")).willReturn(aResponse().withFixedDelay(30000)));

        var alerter = new WebhookAlerter(mockRunContext(), WEBHOOK_URL, DbtCLI.AlertLevel.WARN);
        alerter.maybeAlert("warn", "model.my_project.my_model", "Warning", "slow webhook", "2024-01-01T00:00:00Z");

        long start = System.currentTimeMillis();
        alerter.close();
        long elapsed = System.currentTimeMillis() - start;

        assertThat(elapsed, lessThan(7000L));
    }

    private RunContext mockRunContext() {
        var task = DbtCLI.builder()
            .id(IdUtils.create())
            .type(DbtCLI.class.getName())
            .commands(Property.ofValue(List.of("dbt run")))
            .build();
        return TestsUtils.mockRunContext(runContextFactory, task, Map.of());
    }
}
