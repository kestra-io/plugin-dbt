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
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.io.IOException;
import java.time.Duration;

import static org.awaitility.Awaitility.await;

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

    /**
     * Perform an HTTP request using Kestra HttpClient with retry logic.
     *
     * @param runContext     The Kestra execution context.
     * @param requestBuilder The prepared HTTP request builder.
     * @param responseType   The expected response type.
     * @param <RES>          The response class.
     * @return HttpResponse of type RES.
     */
    protected <RES> HttpResponse<RES> request(
        RunContext runContext,
        HttpRequest.HttpRequestBuilder requestBuilder,
        Class<RES> responseType
    ) throws HttpClientException, IllegalVariableEvaluationException, IOException {

        var request = requestBuilder
            .addHeader("Authorization", "Bearer " + runContext.render(this.token).as(String.class).orElseThrow())
            .addHeader("Content-Type", "application/json")
            .build();

        int rMaxRetries = runContext.render(this.maxRetries).as(Integer.class).orElse(3);
        long rInitialDelay = runContext.render(this.initialDelayMs).as(Long.class).orElse(1000L);

        int attempt = 0;

        try (var client = new HttpClient(runContext, options)) {
            while (true) {
                try {
                    HttpResponse<String> response = client.request(request, String.class);

                    RES parsedResponse = MAPPER.readValue(response.getBody(), responseType);
                    return HttpResponse.<RES>builder()
                        .request(request)
                        .body(parsedResponse)
                        .headers(response.getHeaders())
                        .status(response.getStatus())
                        .build();

                } catch (HttpClientException e) {
                    int statusCode = extractStatusCode(e);

                    if ((statusCode == 502 || statusCode == 503 || statusCode == 504) && attempt < rMaxRetries) {
                        long backoff = (long) (rInitialDelay * Math.pow(2, attempt));
                        runContext.logger().warn(
                            "Request failed with status {}. Retrying in {} ms (attempt {}/{})",
                            statusCode, backoff, attempt + 1, rMaxRetries
                        );

                        await()
                            .pollDelay(Duration.ofMillis(backoff))
                            .atMost(Duration.ofMillis(backoff + 50))
                            .until(() -> true);

                        attempt++;
                        continue;
                    }

                    throw e;
                } catch (IOException e) {
                    throw new RuntimeException("Error executing HTTP request", e);
                }
            }
        }
    }

    private int extractStatusCode(HttpClientException e) {
        if (e instanceof HttpClientResponseException ex) {
            return ex.getResponse().getStatus().getCode();
        }
        return -1;
    }
}
