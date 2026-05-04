package io.kestra.plugin.dbt.cli;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicBoolean;

import io.kestra.core.models.tasks.runners.AbstractLogConsumer;
import io.kestra.core.models.tasks.runners.PluginUtilsService;
import io.kestra.core.runners.RunContext;

/**
 * Routes a line to one of two parsers:
 * - `::{...}::` output markers go to {@link PluginUtilsService#parseOut} so vars are exposed on the task output.
 * - everything else goes to {@link LogService#parse} for dbt-specific structured JSON logging.
 *
 * Calling both for every line would double-log non-marker lines, since `parseOut` logs the raw line
 * when no marker is found and `LogService.parse` also logs.
 */
class DbtLogConsumer extends AbstractLogConsumer {
    private final RunContext runContext;
    private final AtomicBoolean hasWarning;

    DbtLogConsumer(RunContext runContext) {
        this(runContext, new AtomicBoolean(false));
    }

    DbtLogConsumer(RunContext runContext, AtomicBoolean hasWarning) {
        this.runContext = runContext;
        this.hasWarning = hasWarning;
    }

    @Override
    public void accept(String line, Boolean isStdErr, Instant instant) {
        if (line != null && line.startsWith("::{") && line.endsWith("}::")) {
            this.outputs.putAll(PluginUtilsService.parseOut(line, runContext.logger(), runContext, isStdErr, instant));
            return;
        }
        LogService.parse(runContext, line, hasWarning);
    }

    @Override
    public void accept(String line, Boolean isStdErr) {
        this.accept(line, isStdErr, null);
    }
}
