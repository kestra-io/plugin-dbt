package io.kestra.plugin.dbt.cloud;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.time.Duration;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.http.HttpRequest;
import io.kestra.core.http.HttpResponse;
import io.kestra.core.http.client.HttpClient;
import io.kestra.core.http.client.HttpClientException;
import io.kestra.core.http.client.HttpClientRequestException;
import io.kestra.core.http.client.HttpClientResponseException;
import io.kestra.core.http.client.configurations.HttpConfiguration;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.models.tasks.retrys.Exponential;
import io.kestra.core.runners.RunContext;
import io.kestra.core.utils.RetryUtils;
import io.kestra.core.models.annotations.PluginProperty;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractDbtCloud extends Task {
    private static final ObjectMapper MAPPER = new ObjectMapper()
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        .registerModule(new JavaTimeModule());

    @Schema(title = "Base URL to select the tenant")
    @NotNull
    @Builder.Default
    Property<String> baseUrl = Property.ofValue("https://cloud.getdbt.com");

    @Schema(title = "Numeric ID of the account")
    @NotNull
    @PluginProperty(group = "main")
    Property<String> accountId;

    @Schema(title = "API key")
    @NotNull
    @PluginProperty(group = "main", secret = true)
    Property<String> token;

    @Schema(title = "The HTTP client configuration")
    HttpConfiguration options;

    @Schema(
        title = "Maximum number of retries in case of transient errors",
        description = "Default: 3"
    )
    @Builder.Default
    Property<Integer> maxRetries = Property.ofValue(3);

    @Schema(
        title = "Initial delay in milliseconds before retrying",
        description = "Default: 1000 ms (1 second)"
    )
    @Builder.Default
    Property<Long> initialDelayMs = Property.ofValue(1000L);

    protected <RES> HttpResponse<RES> request(
        RunContext runContext,
        HttpRequest.HttpRequestBuilder requestBuilder,
        Class<RES> responseType) throws HttpClientException, IllegalVariableEvaluationException, IOException {

        var request = requestBuilder
            .addHeader("Authorization", "Bearer " + runContext.render(this.token).as(String.class).orElseThrow())
            .addHeader("Content-Type", "application/json")
            .build();

        var rMaxRetries = runContext.render(this.maxRetries).as(Integer.class).orElse(3);
        var rInitialDelay = runContext.render(this.initialDelayMs).as(Long.class).orElse(1000L);

        try (var client = new HttpClient(runContext, options)) {
            return RetryUtils.<HttpResponse<RES>, HttpClientException> of(
                Exponential.builder()
                    .delayFactor(2.0)
                    .interval(Duration.ofMillis(rInitialDelay))
                    .maxInterval(Duration.ofSeconds(30))
                    .maxAttempts(rMaxRetries)
                    .build()
            ).run(
                (res, throwable) -> isRetriableTransientError(throwable, request.getMethod()),
                () ->
                {
                    var response = client.request(request, String.class);
                    var parsedResponse = MAPPER.readValue(response.getBody(), responseType);
                    return HttpResponse.<RES> builder()
                        .request(request)
                        .body(parsedResponse)
                        .headers(response.getHeaders())
                        .status(response.getStatus())
                        .build();
                }
            );
        }
    }

    /**
     * Whether an error calling the dbt Cloud API is transient and worth retrying.
     *
     * <p>Read-only methods (GET/HEAD) retry any transient signal: all 5xx, connection failures and
     * timeouts. Other methods retry only the 502/503/504 gateway errors. A plain 500, a timeout or a
     * connection drop is not retried for them, since the request may already have created the run and a
     * retry could start the job twice. A 502 or 504 carries that same residual risk, but the narrow set
     * is kept for backward compatibility. A 429 (rate limited) is retried for any method, since dbt
     * Cloud rejects it before running the request. Other client errors (4xx) are never retried.
     *
     * <p>The core HTTP client wraps a read timeout as {@code RuntimeException(SocketTimeoutException)},
     * so it is matched through the cause.
     */
    static boolean isRetriableTransientError(Throwable throwable, String method) {
        if (throwable == null) {
            return false;
        }

        boolean readOnly = isReadOnlyMethod(method);

        if (throwable instanceof HttpClientResponseException ex) {
            int code = ex.getResponse().getStatus().getCode();
            // 429 (rate limited) is rejected before the request runs, so it is safe to retry for any method.
            if (code == 429) {
                return true;
            }
            if (readOnly) {
                return code >= 500 && code <= 599;
            }
            return code == 502 || code == 503 || code == 504;
        }

        // Transport-level failures below are ambiguous for a write request: the run may already have
        // been created. Retry them only for read-only methods.
        if (!readOnly) {
            return false;
        }

        // Socket and SSL handshake failures are surfaced by the core HTTP client as this type.
        if (throwable instanceof HttpClientRequestException) {
            return true;
        }

        return throwable instanceof SocketTimeoutException
            || throwable.getCause() instanceof SocketTimeoutException;
    }

    // GET/HEAD only. Named for retry-safety, not RFC idempotency: PUT/DELETE are idempotent but are
    // not safe to retry blindly here, so they are treated as write methods.
    private static boolean isReadOnlyMethod(String method) {
        return "GET".equalsIgnoreCase(method) || "HEAD".equalsIgnoreCase(method);
    }
}
