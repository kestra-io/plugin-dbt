package io.kestra.plugin.dbt.cli;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.tasks.scripts.AbstractLogThread;
import org.slf4j.Logger;

import java.io.InputStream;

class DbtLogParser extends AbstractLogThread {
    static final protected ObjectMapper MAPPER = JacksonMapper.ofJson()
        .setSerializationInclusion(JsonInclude.Include.NON_NULL);

    private final RunContext runContext;

    public DbtLogParser(InputStream inputStream, Logger logger, RunContext runContext) {
        super(inputStream);
        this.runContext = runContext;
    }

    @Override
    protected void call(String line) {
        LogService.parse(runContext, line);
    }
}
