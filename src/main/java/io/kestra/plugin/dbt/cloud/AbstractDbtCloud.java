package io.kestra.plugin.dbt.cloud;

import java.io.IOException;
import java.net.ConnectException;
import java.net.NoRouteToHostException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

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
import io.kestra.core.http.client.configurations.HttpMethod;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.models.tasks.retrys.Exponential;
import io.kestra.core.runners.RunContext;

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
    @ToString.Exclude
    Property<String> token;

    @Schema(title = "The HTTP client configuration")
    HttpConfiguration options;

    @Schema(
        title = "Maximum number of retries in case of transient errors",
        description = "Default: 3. Deprecated: set `options.retry` (e.g. an `Exponential` or `Constant` " +
            "policy) instead. Ignored once `options` is set; kept for backward compatibility with existing " +
            "flows and will be removed in a future major version."
    )
    @Deprecated(since = "1.4", forRemoval = true)
    @Builder.Default
    Property<Integer> maxRetries = Property.ofValue(3);

    @Schema(
        title = "Initial delay in milliseconds before retrying",
        description = "Default: 1000 ms (1 second). Deprecated: set `options.retry.interval` instead. " +
            "Ignored once `options` is set; kept for backward compatibility with existing flows and will " +
            "be removed in a future major version."
    )
    @Deprecated(since = "1.4", forRemoval = true)
    @Builder.Default
    Property<Long> initialDelayMs = Property.ofValue(1000L);

    // dbt Cloud rejects a rate-limited request before running it, so it is always safe to retry for any method.
    private static final int TOO_MANY_REQUESTS = 429;

    // Gateway errors retried for a write.
    private static final Set<Integer> RETRIABLE_WRITE_GATEWAY_CODES = Set.of(502, 503, 504);

    // Gateway errors that may mean dbt Cloud already received the write and created the run.
    private static final Set<Integer> AMBIGUOUS_WRITE_GATEWAY_CODES = Set.of(502, 504);

    // Read-only methods (GET/HEAD) may safely retry any 5xx errors.
    private static final List<Integer> READ_ONLY_RETRIABLE_CODES = Stream.concat(
        Stream.of(TOO_MANY_REQUESTS),
        IntStream.rangeClosed(500, 599).boxed()
    ).collect(Collectors.toUnmodifiableList());

    private static final List<Integer> WRITE_RETRIABLE_CODES = Stream.concat(
        Stream.of(TOO_MANY_REQUESTS),
        RETRIABLE_WRITE_GATEWAY_CODES.stream()
    ).collect(Collectors.toUnmodifiableList());

    private static final List<HttpMethod> TRANSPORT_RETRIABLE_METHODS = List.of(HttpMethod.GET, HttpMethod.HEAD);

    protected <RES> HttpResponse<RES> request(
        RunContext runContext,
        HttpRequest.HttpRequestBuilder requestBuilder,
        Class<RES> responseType) throws HttpClientException, IllegalVariableEvaluationException, IOException {
        return this.request(runContext, requestBuilder, responseType, false);
    }

    // Same as above but with a caller-supplied retry decision (throwable, method) -> retry. Used by callers
    // that can recover an ambiguous write differently (e.g. TriggerRun confirming and adopting the run it may
    // already have created) and so must not let the generic retry re-send it.
    protected <RES> HttpResponse<RES> request(
        RunContext runContext,
        HttpRequest.HttpRequestBuilder requestBuilder,
        Class<RES> responseType,
        boolean reattachEnabled) throws HttpClientException, IllegalVariableEvaluationException, IOException {

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

        var effectiveOptions = retryConfiguration(rMaxRetries, rInitialDelay, reattachEnabled);

        try (var client = new HttpClient(runContext, effectiveOptions)) {
            try {
                var response = client.request(request, String.class);
                // A success status with an empty body cannot be parsed. Throw rather than return a
                // null-bodied response: callers that expect a body (e.g. artifact download) fail loudly
                // instead of silently writing a "null" artifact, and the trigger POST sees an IOException,
                // which isAmbiguousFailure treats as ambiguous so it confirms the run it may have created.
                // readValue(null, ...) would itself throw an opaque IllegalArgumentException, so guard first.
                var body = response.getBody();
                if (body == null || body.isBlank()) {
                    throw new IOException("Empty response body from dbt Cloud");
                }
                var parsedResponse = MAPPER.readValue(body, responseType);
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
    }

    // Write retriable error codes
    private static final List<Integer> WRITE_RETRIABLE_CODES_EXCLUDING_AMBIGUOUS = List.of(TOO_MANY_REQUESTS, 503);

    /**
     * Builds the {@link HttpConfiguration} used for a single call: the user-supplied {@link #options} (or
     * defaults) with the retry policy and retryable status codes overridden to match this class's previous
     * {@code RetryUtils}-based behavior — now expressed entirely through Kestra core's HTTP client retry
     * support instead of a plugin-local retry loop. When reattachEnabled is true, the
     * non-GET/HEAD (write) status-code set drops 502/504, leaving them to surface to the caller; every
     * other retry rule (429, 503, TLS handshake, refused connection, and the full 5xx set for GET/HEAD) is
     * unaffected.
     */
    HttpConfiguration retryConfiguration(int maxAttempts, long initialDelayMs, boolean reattachEnabled) {
        var builder = this.options != null ? this.options.toBuilder() : HttpConfiguration.builder();

        List<Integer> writeCodes = reattachEnabled
            ? WRITE_RETRIABLE_CODES_EXCLUDING_AMBIGUOUS
            : WRITE_RETRIABLE_CODES;

        return builder
            .retry(Exponential.builder()
                .delayFactor(2.0)
                .interval(Duration.ofMillis(initialDelayMs))
                .maxInterval(Duration.ofSeconds(30))
                .maxAttempts(maxAttempts)
                .build())
            .retryOnStatusCodes(Property.ofValue(writeCodes))
            .retryOnStatusCodesByMethod(Property.ofValue(Map.of(
                HttpMethod.GET, READ_ONLY_RETRIABLE_CODES,
                HttpMethod.HEAD, READ_ONLY_RETRIABLE_CODES
            )))
            .retryableTransportFailureMethods(Property.ofValue(TRANSPORT_RETRIABLE_METHODS))
            .build();
    }

    /**
     * Whether a failed write call had an ambiguous outcome: it may already have reached dbt Cloud and
     * created the run, so callers can look it up and adopt it rather than fail. True for a read timeout,
     * a mid-flight drop, or a 502/504 gateway error, whose fate is unknown. Any other HTTP response
     * (a 4xx, a plain 500, a 503) is a definitive answer and returns false, as do a TLS handshake
     * failure, a refused connection, a DNS resolution failure, and a no-route-to-host error, since none
     * of these ever put a byte on the wire. A generic {@link java.net.SocketException} (e.g. a connection
     * reset mid-flight) is deliberately NOT excluded, since the request may already have reached dbt
     * Cloud. A 200 whose body fails to parse also returns true (it looks like an {@link IOException}),
     * which is harmless since the run really was created.
     */
    static boolean isAmbiguousFailure(Throwable throwable) {
        if (throwable == null) {
            return false;
        }

        if (throwable instanceof HttpClientResponseException ex) {
            // A 502/504 gateway error may mean dbt Cloud received the request behind the proxy; any other
            // response is a definitive answer, so it is not ambiguous.
            return AMBIGUOUS_WRITE_GATEWAY_CODES.contains(ex.getResponse().getStatus().getCode());
        }

        if (
            hasCause(throwable, SSLHandshakeException.class)
                || hasCause(throwable, ConnectException.class)
                || hasCause(throwable, UnknownHostException.class)
                || hasCause(throwable, NoRouteToHostException.class)
        ) {
            return false;
        }

        return hasCause(throwable, SocketTimeoutException.class) || hasCause(throwable, IOException.class);
    }

    /**
     * This method returns true for any failure that is likely to be transient and worth retrying, including
     * a 429 rate-limit response, any 5xx response, a TLS handshake failure, a refused connection, a DNS resolution failure, a no-route-to-host error, a socket timeout, or a generic IOException.
     * It returns false for any other failure, including a 4xx response
     */
    static boolean isRetriableReadFailure(Throwable throwable) {
        if (throwable == null) {
            return false;
        }

        if (throwable instanceof HttpClientResponseException ex) {
            int code = ex.getResponse().getStatus().getCode();
            return code == TOO_MANY_REQUESTS || (code >= 500 && code <= 599);
        }

        return throwable instanceof HttpClientRequestException
            || hasCause(throwable, SSLHandshakeException.class)
            || hasCause(throwable, ConnectException.class)
            || hasCause(throwable, UnknownHostException.class)
            || hasCause(throwable, NoRouteToHostException.class)
            || hasCause(throwable, SocketTimeoutException.class)
            || hasCause(throwable, IOException.class);
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
