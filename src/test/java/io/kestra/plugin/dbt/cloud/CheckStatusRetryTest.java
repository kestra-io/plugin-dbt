package io.kestra.plugin.dbt.cloud;

import io.kestra.core.http.HttpRequest;
import io.kestra.core.http.HttpResponse;
import io.kestra.core.http.client.HttpClient;
import io.kestra.core.http.client.HttpClientResponseException;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.net.URI;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@KestraTest
class CheckStatusRetryTest {

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void shouldRetryAndEventuallySucceed() throws Exception {
        var runContext = runContextFactory.of(Map.of());
        var requestBuilder = HttpRequest.builder()
            .uri(new URI("https://fake.api/dbt"));

        try (var mocked = Mockito.mockConstruction(HttpClient.class,
            (mockClient, context) -> when(mockClient.request(any(HttpRequest.class), eq(String.class)))
                .thenThrow(new HttpClientResponseException(
                    "Bad Gateway",
                    HttpResponse.<String>builder()
                        .status(HttpResponse.Status.builder().code(502).build())
                        .build()
                ))
                .thenThrow(new HttpClientResponseException(
                    "Service Unavailable",
                    HttpResponse.<String>builder()
                        .status(HttpResponse.Status.builder().code(503).build())
                        .build()
                ))
                .thenReturn(HttpResponse.<String>builder()
                    .status(HttpResponse.Status.builder().code(200).build())
                    .body("{\"status\":\"ok\"}")
                    .build()))) {

            var task = CheckStatus.builder()
                .id(IdUtils.create())
                .type(CheckStatus.class.getName())
                .runId(Property.ofValue("123"))
                .token(Property.ofValue("fake-token"))
                .accountId(Property.ofValue("fake-account"))
                .maxRetries(Property.ofValue(3))
                .initialDelayMs(Property.ofValue(100L))
                .build();

            var response = task.request(runContext, requestBuilder, Map.class);

            assertEquals(200, response.getStatus().getCode());
            assertEquals("ok", response.getBody().get("status"));

            var mockClient = mocked.constructed().getFirst();
            verify(mockClient, times(3)).request(any(HttpRequest.class), eq(String.class));
        }
    }

    @Test
    void shouldFailAfterMaxRetries() throws Exception {
        var runContext = runContextFactory.of(Map.of());
        var requestBuilder = HttpRequest.builder()
            .uri(new URI("https://fake.api/dbt"));

        try (var mocked = Mockito.mockConstruction(HttpClient.class,
            (mockClient, context) -> when(mockClient.request(any(HttpRequest.class), eq(String.class)))
                .thenThrow(new HttpClientResponseException(
                    "Bad Gateway",
                    HttpResponse.<String>builder()
                        .status(HttpResponse.Status.builder().code(502).build())
                        .build()
                ))
                .thenThrow(new HttpClientResponseException(
                    "Bad Gateway",
                    HttpResponse.<String>builder()
                        .status(HttpResponse.Status.builder().code(502).build())
                        .build()
                ))
                .thenThrow(new HttpClientResponseException(
                    "Bad Gateway",
                    HttpResponse.<String>builder()
                        .status(HttpResponse.Status.builder().code(502).build())
                        .build()
                )))) {

            var task = CheckStatus.builder()
                .id(IdUtils.create())
                .type(CheckStatus.class.getName())
                .runId(Property.ofValue("123"))
                .token(Property.ofValue("fake-token"))
                .accountId(Property.ofValue("fake-account"))
                .maxRetries(Property.ofValue(2))
                .initialDelayMs(Property.ofValue(100L))
                .build();

            assertThrows(HttpClientResponseException.class,
                () -> task.request(runContext, requestBuilder, Map.class)
            );

            var mockClient = mocked.constructed().getFirst();
            verify(mockClient, times(3)).request(any(HttpRequest.class), eq(String.class));
        }
    }
}
