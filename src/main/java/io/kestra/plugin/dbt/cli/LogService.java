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

    protected static void parse(RunContext runContext, String line, AtomicBoolean hasWarning) {
        parse(runContext, line, hasWarning, null);
    }

    @SuppressWarnings("unchecked")
    protected static void parse(RunContext runContext, String line, AtomicBoolean hasWarning, WebhookAlerter alerter) {
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
                additional.putAll(jsonLog);
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
                    if (alerter != null) {
                        alerter.maybeAlert("warn", node(jsonLog), type, msg, ts);
                    }
                    break;
                default:
                    runContext.logger().error(format, (Object[]) args);
                    if (alerter != null) {
                        alerter.maybeAlert("error", node(jsonLog), type, msg, ts);
                    }
            }
        } catch (Throwable e) {
            runContext.logger().info(line.trim());
        }
    }

    /**
     * Extracts the node unique_id (falling back to node_name) a log line refers to, if any. Classic dbt JSON
     * logs nest it under {@code data.node_info}; Fusion's flat format carries the equivalent block at the
     * top level. Returns {@code null} when the line is not tied to a specific node (e.g. a run-level log).
     */
    private static String node(Map<String, Object> jsonLog) {
        if (jsonLog.get("data") instanceof Map<?, ?> data && data.get("node_info") instanceof Map<?, ?> nodeInfo) {
            String node = nodeFromInfo(nodeInfo);
            if (node != null) {
                return node;
            }
        }
        return jsonLog.get("node_info") instanceof Map<?, ?> nodeInfo ? nodeFromInfo(nodeInfo) : null;
    }

    private static String nodeFromInfo(Map<?, ?> nodeInfo) {
        if (nodeInfo.get("unique_id") instanceof String s && !s.isBlank()) {
            return s;
        }
        return nodeInfo.get("node_name") instanceof String s && !s.isBlank() ? s : null;
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
