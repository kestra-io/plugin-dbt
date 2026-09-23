package io.kestra.plugin.dbt.cli;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.Test;

import com.github.tomakehurst.wiremock.junit5.WireMockTest;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;

import jakarta.inject.Inject;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@KestraTest
@WireMockTest(httpPort = 28283)
class LogServiceTest {
    @Inject
    private RunContextFactory runContextFactory;

    private static final String WEBHOOK_URL = "http://localhost:28283/hooks/alert";

    @Test
    void parse_classicInfoFormat_shouldNotSetWarning() {
        var runContext = mockRunContext();
        var hasWarning = new AtomicBoolean(false);
        // Classic dbt JSON log format with nested "info" block
        var line = """
            {"info":{"category":"","code":"Q011","invocation_id":"abc","level":"info","log_version":3,"msg":"Found 1 model","name":"MainReportArgs","pid":1,"thread":null,"ts":"2024-01-01T00:00:00Z"},"data":{}}
            """
            .trim();

        LogService.parse(runContext, line, hasWarning);

        assertThat(hasWarning.get(), is(false));
    }

    @Test
    void parse_classicWarnFormat_shouldSetWarning() {
        var runContext = mockRunContext();
        var hasWarning = new AtomicBoolean(false);
        var line = """
            {"info":{"category":"","code":"W001","invocation_id":"abc","level":"warn","log_version":3,"msg":"Deprecation warning","name":"DeprecatedModel","pid":1,"thread":null,"ts":"2024-01-01T00:00:00Z"},"data":{}}
            """
            .trim();

        LogService.parse(runContext, line, hasWarning);

        assertThat(hasWarning.get(), is(true));
    }

    @Test
    void parse_fusionFormat_shouldNotSetWarning() {
        var runContext = mockRunContext();
        var hasWarning = new AtomicBoolean(false);
        // Fusion v2.0 JSON log format: flat with "message" key instead of nested "info"
        var line = """
            {"level":"info","ts":"2024-01-01T00:00:00Z","name":"MainReportArgs","message":"Found 2 models","thread_name":"MainThread","pid":42}
            """.trim();

        LogService.parse(runContext, line, hasWarning);

        assertThat(hasWarning.get(), is(false));
    }

    @Test
    void parse_fusionWarningLevel_shouldSetWarning() {
        var runContext = mockRunContext();
        var hasWarning = new AtomicBoolean(false);
        // Fusion may emit "warning" (full word) instead of "warn"
        var line = """
            {"level":"warning","ts":"2024-01-01T00:00:00Z","name":"Deprecation","message":"This feature is deprecated","thread_name":"MainThread","pid":42}
            """.trim();

        LogService.parse(runContext, line, hasWarning);

        assertThat(hasWarning.get(), is(true));
    }

    @Test
    void parse_classicWarnFormat_reachesAlerterWithNode() {
        stubFor(post(urlEqualTo("/hooks/alert")).willReturn(ok()));
        var runContext = mockRunContext();
        var hasWarning = new AtomicBoolean(false);
        var line = """
            {"info":{"level":"warn","ts":"2024-01-01T00:00:00Z","thread":null,"name":"DeprecatedModel","msg":"Deprecation warning"},"data":{"node_info":{"unique_id":"model.my_project.my_model","node_name":"my_model"}}}
            """
            .trim();

        try (var alerter = new WebhookAlerter(runContext, WEBHOOK_URL, DbtCLI.AlertLevel.WARN)) {
            LogService.parse(runContext, line, hasWarning, alerter);
        }

        assertThat(hasWarning.get(), is(true));
        verify(
            1,
            postRequestedFor(urlEqualTo("/hooks/alert"))
                .withRequestBody(matchingJsonPath("$.level", equalTo("warn")))
                .withRequestBody(matchingJsonPath("$.node", equalTo("model.my_project.my_model")))
        );
    }

    @Test
    void parse_fusionErrorFormat_reachesAlerterWithNode() {
        stubFor(post(urlEqualTo("/hooks/alert")).willReturn(ok()));
        var runContext = mockRunContext();
        var hasWarning = new AtomicBoolean(false);
        // Fusion v2.0 flat format carries node_info at the top level instead of nested under "data"
        var line = """
            {"level":"error","ts":"2024-01-01T00:00:00Z","thread_name":"MainThread","name":"CompilationError","message":"Model failed to compile","node_info":{"unique_id":"model.my_project.my_model"}}
            """
            .trim();

        try (var alerter = new WebhookAlerter(runContext, WEBHOOK_URL, DbtCLI.AlertLevel.WARN)) {
            LogService.parse(runContext, line, hasWarning, alerter);
        }

        verify(
            1,
            postRequestedFor(urlEqualTo("/hooks/alert"))
                .withRequestBody(matchingJsonPath("$.level", equalTo("error")))
                .withRequestBody(matchingJsonPath("$.node", equalTo("model.my_project.my_model")))
        );
    }

    @Test
    void parse_nonJsonLine_shouldNotThrow() {
        var runContext = mockRunContext();
        var hasWarning = new AtomicBoolean(false);

        LogService.parse(runContext, "plain text output", hasWarning);

        assertThat(hasWarning.get(), is(false));
    }

    @Test
    void parse_nullLine_shouldNotThrow() {
        var runContext = mockRunContext();
        var hasWarning = new AtomicBoolean(false);

        LogService.parse(runContext, null, hasWarning);

        assertThat(hasWarning.get(), is(false));
    }

    private io.kestra.core.runners.RunContext mockRunContext() {
        var task = DbtCLI.builder()
            .id(IdUtils.create())
            .type(DbtCLI.class.getName())
            .commands(Property.ofValue(List.of("dbt run")))
            .build();
        return TestsUtils.mockRunContext(runContextFactory, task, Map.of());
    }
}
