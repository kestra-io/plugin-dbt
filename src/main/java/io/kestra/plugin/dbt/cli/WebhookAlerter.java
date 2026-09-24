package io.kestra.plugin.dbt.cli;

import java.net.URI;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import io.kestra.core.http.HttpRequest;
import io.kestra.core.http.client.HttpClient;
import io.kestra.core.http.client.configurations.HttpConfiguration;
import io.kestra.core.http.client.configurations.TimeoutConfiguration;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;

/**
 * Posts a Slack-compatible alert for each distinct WARN/ERROR dbt log line, off the log-consumer thread.
 *
 * <p>A single background thread drains a bounded queue so the dbt process output is never slowed down by a
 * slow or unreachable webhook. Alerts are deduplicated per run (same level + node) and capped at
 * {@value #MAX_ALERTS}; anything beyond that is silently suppressed and reported once, as a summary, on
 * {@link #close()}.
 */
class WebhookAlerter implements AutoCloseable {
    private static final int QUEUE_CAPACITY = 100;
    // package-private: read by WebhookAlerterTest to size its cap-exceeded scenario without duplicating the value
    static final int MAX_ALERTS = 20;
    private static final Duration HTTP_TIMEOUT = Duration.ofSeconds(5);
    private static final Duration CLOSE_TIMEOUT = Duration.ofSeconds(5);

    private static final HttpConfiguration HTTP_CONFIGURATION = HttpConfiguration.builder()
        .timeout(
            TimeoutConfiguration.builder()
                .connectTimeout(Property.ofValue(HTTP_TIMEOUT))
                .readIdleTimeout(Property.ofValue(HTTP_TIMEOUT))
                .build()
        )
        .build();

    // Sentinel enqueued by close() to stop the worker loop once every alert already queued has run.
    private static final Runnable POISON_PILL = () -> {};

    private final RunContext runContext;
    private final String url;
    private final DbtCLI.AlertLevel minLevel;
    private final BlockingQueue<Runnable> queue = new LinkedBlockingQueue<>(QUEUE_CAPACITY);
    private final Set<String> seenKeys = ConcurrentHashMap.newKeySet();
    private final Thread worker;

    private int sentCount = 0;
    private int suppressedCount = 0;

    WebhookAlerter(RunContext runContext, String url, DbtCLI.AlertLevel minLevel) {
        this.runContext = runContext;
        this.url = url;
        this.minLevel = minLevel;
        this.worker = new Thread(this::runLoop, "dbt-cli-webhook-alerter");
        this.worker.setDaemon(true);
        this.worker.start();
    }

    private void runLoop() {
        try {
            Runnable task;
            while ((task = queue.take()) != POISON_PILL) {
                task.run();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Called from the log-consumer thread for every parsed WARN/ERROR line. Never blocks: a full queue drops
     * the alert and counts it as suppressed rather than stalling dbt's output stream.
     */
    synchronized void maybeAlert(String level, String node, String type, String msg, String ts) {
        DbtCLI.AlertLevel alertLevel = "warn".equals(level)
            ? DbtCLI.AlertLevel.WARN
            : "error".equals(level) ? DbtCLI.AlertLevel.ERROR : null;

        if (alertLevel == null || alertLevel.ordinal() < minLevel.ordinal()) {
            return;
        }

        String key = node != null ? level + ":" + node : level + ":" + type + ":" + msg;
        if (!seenKeys.add(key)) {
            return;
        }

        if (sentCount >= MAX_ALERTS) {
            suppressedCount++;
            return;
        }

        if (queue.offer(() -> send(payload(alertLevel, node, type, msg, ts)))) {
            sentCount++;
        } else {
            suppressedCount++;
        }
    }

    private Map<String, Object> payload(DbtCLI.AlertLevel level, String node, String type, String msg, String ts) {
        var rLevel = level.toString().toLowerCase();
        var payload = new LinkedHashMap<String, Object>();
        payload.put("text", rLevel + (node != null ? " " + node : "") + ": " + msg);
        payload.put("level", rLevel);
        putIfPresent(payload, "node", node);
        putIfPresent(payload, "type", type);
        putIfPresent(payload, "msg", msg);
        putIfPresent(payload, "ts", ts);
        putIfPresent(payload, "namespace", runContext.flowInfo().namespace());
        putIfPresent(payload, "flowId", runContext.flowInfo().id());
        putIfPresent(payload, "executionId", runContext.taskRunInfo().executionId());
        putIfPresent(payload, "taskId", runContext.taskRunInfo().taskId());
        return payload;
    }

    private static void putIfPresent(Map<String, Object> payload, String key, String value) {
        if (value != null && !value.isBlank()) {
            payload.put(key, value);
        }
    }

    private void send(Map<String, Object> payload) {
        try (var client = new HttpClient(runContext, HTTP_CONFIGURATION)) {
            var request = HttpRequest.builder()
                .uri(URI.create(url))
                .method("POST")
                .body(HttpRequest.JsonRequestBody.of(payload))
                .build();
            // Response body is never used: the Consumer overload streams it instead of materializing a String,
            // so a misconfigured URL returning a large body never gets buffered in memory.
            client.request(request, response -> {});
        } catch (Exception e) {
            // Never surface a webhook failure to the task: the exception message from the HTTP client can
            // embed the URI (which may carry a secret token), so only the exception class is logged.
            runContext.logger().debug("dbt alertWebhook POST failed: {}", e.getClass().getSimpleName());
        }
    }

    @Override
    public void close() {
        synchronized (this) {
            if (suppressedCount > 0) {
                var summary = suppressedCount + " more dbt warnings/errors suppressed";
                queue.offer(() -> send(Map.of("text", summary, "level", minLevel.toString().toLowerCase())));
            }
        }
        queue.offer(POISON_PILL);

        try {
            worker.join(CLOSE_TIMEOUT.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        if (worker.isAlive()) {
            worker.interrupt();
        }
    }
}
