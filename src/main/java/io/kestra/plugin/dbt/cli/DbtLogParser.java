package io.kestra.plugin.dbt.cli;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.tasks.scripts.AbstractLogThread;
import org.slf4j.Logger;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

class DbtLogParser extends AbstractLogThread {
    static final protected ObjectMapper MAPPER = JacksonMapper.ofJson()
        .setSerializationInclusion(JsonInclude.Include.NON_NULL);

    private final Logger logger;
    private final RunContext runContext;

    public DbtLogParser(InputStream inputStream, Logger logger, RunContext runContext) {
        super(inputStream);
        this.logger = logger;
        this.runContext = runContext;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected void call(String line) {
        try {
            Map<String, Object> jsonLog = (Map<String, Object>) MAPPER.readValue(line, Object.class);

            HashMap<String, Object> additional = new HashMap<>(jsonLog);
            additional.remove("ts");
            additional.remove("pid");
            additional.remove("invocation_id");
            additional.remove("level");
            additional.remove("log_version");
            additional.remove("code");
            additional.remove("msg");
            additional.remove("thread_name");
            additional.remove("type");

            String format = "[Date: {}] [Thread: {}] [Type: {}] {}{}";
            String[] args = new String[]{
                (String) jsonLog.get("ts"),
                (String) jsonLog.get("thread_name"),
                (String) jsonLog.get("type"),
                jsonLog.get("msg") != null ? jsonLog.get("msg") + " " : "",
                additional.size() > 0 ? additional.toString() : ""
            };

            if (jsonLog.containsKey("data")) {
                Map<String, Object> data = (Map<String, Object>) jsonLog.get("data");

                if (data.containsKey("stats")) {
                    Map<String, Integer> stats  = (Map<String, Integer>) data.get("stats");

                    stats.forEach((s, integer) -> {
                        runContext.metric(Counter.of(s, integer));
                    });
                }
            }

            switch ((String) jsonLog.get("level")) {
                case "debug":
                    logger.debug(format, (Object[]) args);
                    break;
                case "info":
                    logger.info(format, (Object[]) args);
                    break;
                case "warning":
                    logger.warn(format, (Object[]) args);
                    break;
                default:
                    logger.error(format, (Object[]) args);
            }
        } catch (Throwable e) {
            logger.info(line.trim());
        }
    }
}
