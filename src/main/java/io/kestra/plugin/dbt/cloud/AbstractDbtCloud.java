package io.kestra.plugin.dbt.cloud;

import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import jakarta.validation.constraints.NotNull;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractDbtCloud extends Task {
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

    protected <T> HttpResponse<String> request(RunContext runContext,
                                               HttpRequest.Builder requestBuilder,
                                               Duration timeout) {
        try {
            String token = runContext.render(this.token).as(String.class).orElseThrow();
            String baseUrl = runContext.render(this.baseUrl).as(String.class).orElseThrow();

            HttpRequest request = requestBuilder
                .uri(URI.create(baseUrl))
                .header("Authorization", "Bearer " + token)
                .header("Content-Type", "application/json")
                .timeout(timeout != null ? timeout : HTTP_READ_TIMEOUT)
                .build();

            try {
                return CLIENT.send(request, HttpResponse.BodyHandlers.ofString());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Request interrupted", e);
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to execute request", e);
        }
    }

    protected <T> CompletableFuture<HttpResponse<String>> requestAsync(RunContext runContext,
                                                                       HttpRequest.Builder requestBuilder,
                                                                       Duration timeout) {
        try {
            String token = runContext.render(this.token).as(String.class).orElseThrow();
            String baseUrl = runContext.render(this.baseUrl).as(String.class).orElseThrow();

            HttpRequest request = requestBuilder
                .uri(URI.create(baseUrl))
                .header("Authorization", "Bearer " + token)
                .header("Content-Type", "application/json")
                .timeout(timeout != null ? timeout : HTTP_READ_TIMEOUT)
                .build();

            return CLIENT.sendAsync(request, HttpResponse.BodyHandlers.ofString());
        } catch (Exception e) {
            CompletableFuture<HttpResponse<String>> future = new CompletableFuture<>();
            future.completeExceptionally(new RuntimeException("Failed to execute request", e));
            return future;
        }
    }
}