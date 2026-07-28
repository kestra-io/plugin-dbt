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
     * <p>Idempotent reads (GET/HEAD) retry any transient signal: all 5xx, connection failures and
     * timeouts. Non-idempotent methods retry only 502/503/504, which mean the request never reached the
     * app; a 500, timeout or connection drop could mean the run was already created, so retrying could
     * start the job twice. Client errors (4xx) are never retried.
     *
     * <p>The core HTTP client wraps a read timeout as {@code RuntimeException(SocketTimeoutException)},
     * so it is matched through the cause.
     */
    static boolean isRetriableTransientError(Throwable throwable, String method) {
        if (throwable == null) {
            return false;
        }

        boolean idempotent = isIdempotent(method);

        if (throwable instanceof HttpClientResponseException ex) {
            int code = ex.getResponse().getStatus().getCode();
            if (idempotent) {
                return code >= 500 && code <= 599;
            }
            return code == 502 || code == 503 || code == 504;
        }

        // Transport-level failures below are ambiguous for a non-idempotent request, so only retry
        // them for idempotent reads.
        if (!idempotent) {
            return false;
        }

        // Socket and SSL handshake failures are surfaced by the core HTTP client as this type.
        if (throwable instanceof HttpClientRequestException) {
            return true;
        }

        return throwable instanceof SocketTimeoutException
            || throwable.getCause() instanceof SocketTimeoutException;
    }

    private static boolean isIdempotent(String method) {
        return "GET".equalsIgnoreCase(method) || "HEAD".equalsIgnoreCase(method);
    }
}
