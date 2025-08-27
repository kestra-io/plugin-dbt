package io.kestra.plugin.dbt.cloud;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.http.HttpRequest;
import io.kestra.core.http.HttpResponse;
import io.kestra.core.http.client.HttpClient;
import io.kestra.core.http.client.HttpClientException;
import io.kestra.core.http.client.HttpClientResponseException;
import io.kestra.core.http.client.configurations.HttpConfiguration;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.models.tasks.retrys.Exponential;
import io.kestra.core.runners.RunContext;
import io.kestra.core.utils.RetryUtils;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.io.IOException;
import java.time.Duration;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractDbtCloud extends Task {
    private static final ObjectMapper MAPPER = new ObjectMapper()
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        .registerModule(new JavaTimeModule());

    @Schema(title = "Base URL to select the tenant.")
    @NotNull
    @Builder.Default
    Property<String> baseUrl = Property.ofValue("https://cloud.getdbt.com");

    @Schema(title = "Numeric ID of the account.")
    @NotNull
    Property<String> accountId;

    @Schema(title = "API key.")
    @NotNull
    Property<String> token;

    @Schema(title = "The HTTP client configuration.")
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
        Class<RES> responseType
    ) throws HttpClientException, IllegalVariableEvaluationException, IOException {

        var request = requestBuilder
            .addHeader("Authorization", "Bearer " + runContext.render(this.token).as(String.class).orElseThrow())
            .addHeader("Content-Type", "application/json")
            .build();

        var rMaxRetries = runContext.render(this.maxRetries).as(Integer.class).orElse(3);
        var rInitialDelay = runContext.render(this.initialDelayMs).as(Long.class).orElse(1000L);

        try (var client = new HttpClient(runContext, options)) {
            return new RetryUtils().<HttpResponse<RES>, HttpClientException>of(
                Exponential.builder()
                    .delayFactor(2.0)
                    .interval(Duration.ofMillis(rInitialDelay))
                    .maxInterval(Duration.ofSeconds(30))
                    .maxAttempts(rMaxRetries)
                    .build()
            ).run(
                (res, throwable) -> throwable instanceof HttpClientResponseException ex &&
                    (ex.getResponse().getStatus().getCode() == 502 ||
                        ex.getResponse().getStatus().getCode() == 503 ||
                        ex.getResponse().getStatus().getCode() == 504),
                () -> {
                    var response = client.request(request, String.class);
                    var parsedResponse = MAPPER.readValue(response.getBody(), responseType);
                    return HttpResponse.<RES>builder()
                        .request(request)
                        .body(parsedResponse)
                        .headers(response.getHeaders())
                        .status(response.getStatus())
                        .build();
                }
            );
        }
    }
}
