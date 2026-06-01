package io.kestra.plugin.dbt.cli;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;

class LogService {
    static final protected ObjectMapper MAPPER = JacksonMapper.ofJson()
        .setSerializationInclusion(JsonInclude.Include.NON_NULL);

    @SuppressWarnings("unchecked")
    protected static void parse(RunContext runContext, String line, AtomicBoolean hasWarning) {
        if (line == null) {
            return;
        }
        try {
            Map<String, Object> jsonLog = (Map<String, Object>) MAPPER.readValue(line, Object.class);

            String level;
            String ts;
            String thread;
            String type;
            String msg;
            HashMap<String, Object> additional = new HashMap<>();

            if (jsonLog.containsKey("info")) {
                // Classic dbt JSON log format: { "info": { "level": ..., "ts": ..., ... }, "data": { ... } }
                Map<String, Object> infoLog = (Map<String, Object>) jsonLog.get("info");

                level = (String) infoLog.get("level");
                ts = (String) infoLog.get("ts");
                thread = (String) infoLog.get("thread");
                type = (String) infoLog.get("name");
                msg = (String) infoLog.get("msg");

                additional.putAll(infoLog);
            } else if (jsonLog.containsKey("message")) {
                // Fusion v2.0 JSON log format: { "level": ..., "ts": ..., "message": ..., ... }
                level = normalizeLevel((String) jsonLog.get("level"));
                ts = (String) jsonLog.get("ts");
                thread = (String) jsonLog.get("thread_name");
                type = (String) jsonLog.get("name");
                msg = (String) jsonLog.get("message");
            } else {
                // Legacy flat JSON log format
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
            additional.remove("message");
            additional.remove("thread");
            additional.remove("thread_name");
            additional.remove("type");
            additional.remove("name");
            additional.remove("ts");
            additional.remove("pid");
            additional.remove("extra");

            String format = "[Date: {}] [Thread: {}] [Type: {}] {}{}";
            String[] args = new String[] {
                ts,
                thread,
                type,
                msg != null ? msg + " " : "",
                !additional.isEmpty() ? additional.toString() : ""
            };

            if (jsonLog.containsKey("data")) {
                Map<String, Object> data = (Map<String, Object>) jsonLog.get("data");

                if (data.containsKey("stats")) {
                    Map<String, Integer> stats = (Map<String, Integer>) data.get("stats");

                    stats.forEach((s, integer) -> runContext.metric(Counter.of(s, integer)));
                }
            }

            if (level == null) {
                runContext.logger().info(format, (Object[]) args);
                return;
            }

            switch (level) {
                case "debug":
                    runContext.logger().debug(format, (Object[]) args);
                    break;
                case "info":
                    runContext.logger().info(format, (Object[]) args);
                    break;
                case "warn":
                    hasWarning.set(true);
                    runContext.logger().warn(format, (Object[]) args);
                    break;
                default:
                    runContext.logger().error(format, (Object[]) args);
            }
        } catch (Throwable e) {
            runContext.logger().info(line.trim());
        }
    }

    /**
     * Fusion v2.0 may emit numeric or uppercase level strings; normalize to lowercase dbt-classic values.
     */
    private static String normalizeLevel(String level) {
        if (level == null) {
            return null;
        }
        return switch (level.toLowerCase()) {
            case "warning" -> "warn";
            case "error", "critical" -> "error";
            case "debug", "trace" -> "debug";
            default -> level.toLowerCase();
        };
    }
}
