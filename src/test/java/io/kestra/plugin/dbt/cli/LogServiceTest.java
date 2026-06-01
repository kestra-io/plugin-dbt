package io.kestra.plugin.dbt.cli;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.dbt.cli.DbtCLI;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@KestraTest
class LogServiceTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void parse_classicInfoFormat_shouldNotSetWarning() {
        var runContext = mockRunContext();
        var hasWarning = new AtomicBoolean(false);
        // Classic dbt JSON log format with nested "info" block
        var line = """
            {"info":{"category":"","code":"Q011","invocation_id":"abc","level":"info","log_version":3,"msg":"Found 1 model","name":"MainReportArgs","pid":1,"thread":null,"ts":"2024-01-01T00:00:00Z"},"data":{}}
            """.trim();

        LogService.parse(runContext, line, hasWarning);

        assertThat(hasWarning.get(), is(false));
    }

    @Test
    void parse_classicWarnFormat_shouldSetWarning() {
        var runContext = mockRunContext();
        var hasWarning = new AtomicBoolean(false);
        var line = """
            {"info":{"category":"","code":"W001","invocation_id":"abc","level":"warn","log_version":3,"msg":"Deprecation warning","name":"DeprecatedModel","pid":1,"thread":null,"ts":"2024-01-01T00:00:00Z"},"data":{}}
            """.trim();

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
