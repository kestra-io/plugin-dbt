package io.kestra.plugin.dbt.cloud;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import jakarta.validation.constraints.NotNull;


import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
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

    @Schema(
        title = "Base url to select the tenant."
    )
    @NotNull
    @Builder.Default
    Property<String> baseUrl = Property.of("https://cloud.getdbt.com");

    @Schema(
        title = "Numeric ID of the account."
    )
    @NotNull
    Property<String> accountId;

    @Schema(
        title = "API key."
    )
    @NotNull
    Property<String> token;

    private static final Duration HTTP_READ_TIMEOUT = Duration.ofSeconds(60);
    private static final HttpClient CLIENT = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(10))
        .build();



    protected <T> T request(RunContext runContext,
                                          HttpRequest.Builder requestBuilder,
                                          Class<T> responseType) throws Exception {
        return request(runContext, requestBuilder, responseType, null);
    }

    protected <T> T request(RunContext runContext,
                                          HttpRequest.Builder requestBuilder,
                                          Class<T> responseType,
                                          Duration timeout) throws Exception {
        String token = runContext.render(this.token).as(String.class).orElseThrow();

        HttpRequest request = requestBuilder
            .header("Authorization", "Bearer " + token)
            .header("Content-Type", "application/json")
            .timeout(timeout != null ? timeout : HTTP_READ_TIMEOUT)
            .build();

        HttpResponse<String> stringResponse = CLIENT.send(request, HttpResponse.BodyHandlers.ofString());
        return MAPPER.readValue(stringResponse.body(), responseType);

    }

    protected String createUrl(RunContext runContext, String path) throws IllegalVariableEvaluationException {
        String baseUrlString = runContext.render(this.baseUrl).as(String.class).orElseThrow();
        baseUrlString = baseUrlString.endsWith("/") ? baseUrlString.substring(0, baseUrlString.length() - 1) : baseUrlString;
        return baseUrlString + (path.startsWith("/") ? path : "/" + path);
    }
}