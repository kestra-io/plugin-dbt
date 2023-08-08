package io.kestra.plugin.dbt.cli;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.tasks.scripts.AbstractLogThread;
import org.slf4j.Logger;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

class LogService {
    static final protected ObjectMapper MAPPER = JacksonMapper.ofJson()
        .setSerializationInclusion(JsonInclude.Include.NON_NULL);

    @SuppressWarnings("unchecked")
    protected static void parse(RunContext runContext, String line) {
        try {
            Map<String, Object> jsonLog = (Map<String, Object>) MAPPER.readValue(line, Object.class);

            String level;
            String ts;
            String thread;
            String type;
            String msg;
            HashMap<String, Object> additional = new HashMap<>();

            if (jsonLog.containsKey("info")) {
                Map<String, Object> infoLog = (Map<String, Object>) jsonLog.get("info");

                level = (String) infoLog.get("level");
                ts = (String) infoLog.get("ts");
                thread = (String) infoLog.get("thread");
                type = (String) infoLog.get("name");
                msg = (String) infoLog.get("msg");

                additional.putAll(infoLog);
            } else {
                level = (String) jsonLog.get("level");
                ts = (String) jsonLog.get("ts");
                thread = (String) jsonLog.get("thread_name");
                type = (String) jsonLog.get("type");
                msg = (String) jsonLog.get("msg");
            }

            additional.remove("category");
            additional.remove("code");
            additional.remove("invocation_id");
            additional.remove("level");
            additional.remove("log_version");
            additional.remove("code");
            additional.remove("msg");
            additional.remove("thread");
            additional.remove("thread_name");
            additional.remove("type");
            additional.remove("name");
            additional.remove("ts");
            additional.remove("pid");
            additional.remove("extra");

            String format = "[Date: {}] [Thread: {}] [Type: {}] {}{}";
            String[] args = new String[]{
                ts,
                thread,
                type,
                msg != null ? msg + " " : "",
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

            switch (level) {
                case "debug":
                    runContext.logger().debug(format, (Object[]) args);
                    break;
                case "info":
                    runContext.logger().info(format, (Object[]) args);
                    break;
                case "warning":
                    runContext.logger().warn(format, (Object[]) args);
                    break;
                default:
                    runContext.logger().error(format, (Object[]) args);
            }
        } catch (Throwable e) {
            runContext.logger().info(line.trim());
        }
    }
}
