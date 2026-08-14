package io.kestra.plugin.dbt.cloud;

import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.time.Duration;

import javax.net.ssl.SSLHandshakeException;

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
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.models.tasks.retrys.Exponential;
import io.kestra.core.runners.RunContext;
import io.kestra.core.utils.RetryUtils;

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

    // Legacy shared host. It no longer resolves tokens for regional-cell accounts, whose access URL
    // instead follows ACCOUNT_PREFIX.REGION.dbt.com. Kept as the default for backward compatibility,
    // but flagged on a 401 (see request()).
    private static final String LEGACY_BASE_URL = "https://cloud.getdbt.com";

    @Schema(
        title = "Base URL to select the tenant",
        description = """
            The access URL for your dbt Cloud account. Regional and cell-based accounts use a URL \
            of the form `ACCOUNT_PREFIX.REGION.dbt.com`, not the legacy `cloud.getdbt.com` host, \
            which no longer resolves tokens for these accounts and returns a 401 error even with a \
            valid token."""
    )
    @NotNull
    @Builder.Default
    Property<String> baseUrl = Property.ofValue(LEGACY_BASE_URL);

    @Schema(
        title = "Numeric ID of the account",
        description = "The numeric dbt Cloud account ID, visible in the account settings and in the dbt Cloud URL."
    )
    @NotNull
    @PluginProperty(group = "main")
    Property<String> accountId;

    @Schema(
        title = "API token",
        description = "A dbt Cloud API token (a Service Account token or a Personal Access token); sent as a Bearer token."
    )
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
        // The legacy default is still valid for non-regional accounts, so it is not flagged up front.
        // Only a genuine 401 against it is worth a hint, emitted in the catch below.
        var usesLegacyBaseUrl = LEGACY_BASE_URL.equals(
            runContext.render(this.baseUrl).as(String.class).orElse(LEGACY_BASE_URL)
        );

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
                    try {
                        var response = client.request(request, String.class);
                        var parsedResponse = MAPPER.readValue(response.getBody(), responseType);
                        return HttpResponse.<RES> builder()
                            .request(request)
                            .body(parsedResponse)
                            .headers(response.getHeaders())
                            .status(response.getStatus())
                            .build();
                    } catch (HttpClientResponseException e) {
                        // A 401 against the legacy default host is almost always a wrong baseUrl, not a bad
                        // token: the shared host no longer resolves tokens for regional and cell-based
                        // accounts. Rethrow the same type with an enriched message so the failure itself
                        // names baseUrl, keeping the original 401 as the cause. Other cases pass through.
                        if (usesLegacyBaseUrl && e.getResponse().getStatus().getCode() == 401) {
                            throw new HttpClientResponseException(
                                "Received a 401 while using the legacy baseUrl default \"" + LEGACY_BASE_URL +
                                    "\". This host no longer resolves tokens for regional and cell-based dbt " +
                                    "Cloud accounts. Before checking the token, set baseUrl to your account's " +
                                    "access URL (ACCOUNT_PREFIX.REGION.dbt.com). Original error: " + e.getMessage(),
                                e.getResponse(),
                                e
                            );
                        }
                        throw e;
                    }
                }
            );
        }
    }

    /**
     * Whether an error calling the dbt Cloud API is transient and worth retrying.
     *
     * <p>
     * Read-only methods (GET/HEAD) retry any transient signal: all 5xx, connection failures and
     * timeouts. Other methods retry only the 502/503/504 gateway errors plus transport failures that
     * provably never reached dbt Cloud (TLS handshake, connection refused). A plain 500, a read timeout
     * or a mid-flight connection drop is not retried for them, since the request may already have
     * created the run and a retry could start the job twice. A 502 or 504 carries that same residual
     * risk, but the narrow set is kept for backward compatibility. A 429 (rate limited) is retried for
     * any method, since dbt Cloud rejects it before running the request. Other client errors (4xx) are
     * never retried.
     *
     * <p>
     * The core HTTP client wraps a read timeout as {@code RuntimeException(SocketTimeoutException)},
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

        // Transport-level failures. For read-only methods any of them is retriable. For write methods
        // only those that provably never reached the app are safe: a TLS handshake failure happens
        // before any request bytes are sent, and a refused connection is never established. A read
        // timeout or a mid-flight connection drop stays ambiguous (the run may already exist), so it
        // is not retried for writes.
        if (!readOnly) {
            return hasCause(throwable, SSLHandshakeException.class)
                || hasCause(throwable, ConnectException.class);
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

    // Walks the cause chain (bounded, to tolerate a cyclic cause) looking for a given exception type.
    private static boolean hasCause(Throwable throwable, Class<? extends Throwable> type) {
        Throwable current = throwable;
        for (int depth = 0; current != null && depth < 16; current = current.getCause(), depth++) {
            if (type.isInstance(current)) {
                return true;
            }
        }
        return false;
    }
}
