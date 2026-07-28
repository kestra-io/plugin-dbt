package io.kestra.plugin.dbt.cloud;

import java.net.SocketTimeoutException;
import java.net.URI;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import io.kestra.core.http.HttpRequest;
import io.kestra.core.http.HttpResponse;
import io.kestra.core.http.client.HttpClient;
import io.kestra.core.http.client.HttpClientResponseException;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.RetryUtils;

import jakarta.inject.Inject;

import static org.junit.jupiter.api.Assertions.*;
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

        try (
            var mocked = Mockito.mockConstruction(
                HttpClient.class,
                (mockClient, context) -> when(mockClient.request(any(HttpRequest.class), eq(String.class)))
                    .thenThrow(
                        new HttpClientResponseException(
                            "Bad Gateway",
                            HttpResponse.<String> builder()
                                .status(HttpResponse.Status.builder().code(502).build())
                                .build()
                        )
                    )
                    .thenThrow(
                        new HttpClientResponseException(
                            "Service Unavailable",
                            HttpResponse.<String> builder()
                                .status(HttpResponse.Status.builder().code(503).build())
                                .build()
                        )
                    )
                    .thenReturn(
                        HttpResponse.<String> builder()
                            .status(HttpResponse.Status.builder().code(200).build())
                            .body("{\"status\":\"ok\"}")
                            .build()
                    )
            )
        ) {

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
    void shouldRetryOnServerErrorAndEventuallySucceed() throws Exception {
        // A transient 500 during status polling (an idempotent GET) must be retried instead of
        // failing the task while the dbt Cloud run is still healthy.
        var runContext = runContextFactory.of(Map.of());
        var requestBuilder = HttpRequest.builder()
            .uri(new URI("https://fake.api/dbt"))
            .method("GET");

        try (
            var mocked = Mockito.mockConstruction(
                HttpClient.class,
                (mockClient, context) -> when(mockClient.request(any(HttpRequest.class), eq(String.class)))
                    .thenThrow(
                        new HttpClientResponseException(
                            "Internal Server Error",
                            HttpResponse.<String> builder()
                                .status(HttpResponse.Status.builder().code(500).build())
                                .build()
                        )
                    )
                    .thenReturn(
                        HttpResponse.<String> builder()
                            .status(HttpResponse.Status.builder().code(200).build())
                            .body("{\"status\":\"ok\"}")
                            .build()
                    )
            )
        ) {

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
            verify(mockClient, times(2)).request(any(HttpRequest.class), eq(String.class));
        }
    }

    @Test
    void shouldNotRetryPostOnServerError() throws Exception {
        // A 500 on the non-idempotent POST /run/ trigger must fail fast, not retry: dbt Cloud may
        // have created the run before failing to respond, so a retry could start a second job.
        var runContext = runContextFactory.of(Map.of());
        var requestBuilder = HttpRequest.builder()
            .uri(new URI("https://fake.api/dbt"))
            .method("POST");

        try (
            var mocked = Mockito.mockConstruction(
                HttpClient.class,
                (mockClient, context) -> when(mockClient.request(any(HttpRequest.class), eq(String.class)))
                    .thenThrow(
                        new HttpClientResponseException(
                            "Internal Server Error",
                            HttpResponse.<String> builder()
                                .status(HttpResponse.Status.builder().code(500).build())
                                .build()
                        )
                    )
            )
        ) {

            var task = CheckStatus.builder()
                .id(IdUtils.create())
                .type(CheckStatus.class.getName())
                .runId(Property.ofValue("123"))
                .token(Property.ofValue("fake-token"))
                .accountId(Property.ofValue("fake-account"))
                .maxRetries(Property.ofValue(3))
                .initialDelayMs(Property.ofValue(100L))
                .build();

            var ex = assertThrows(
                HttpClientResponseException.class,
                () -> task.request(runContext, requestBuilder, Map.class)
            );
            assertEquals(500, ex.getResponse().getStatus().getCode());

            var mockClient = mocked.constructed().getFirst();
            verify(mockClient, times(1)).request(any(HttpRequest.class), eq(String.class));
        }
    }

    @Test
    void shouldRetryPostOnBadGatewayAndEventuallySucceed() throws Exception {
        // 502/503/504 mean the trigger never reached the app, so the non-idempotent POST /run/ is
        // still safe to retry on those.
        var runContext = runContextFactory.of(Map.of());
        var requestBuilder = HttpRequest.builder()
            .uri(new URI("https://fake.api/dbt"))
            .method("POST");

        try (
            var mocked = Mockito.mockConstruction(
                HttpClient.class,
                (mockClient, context) -> when(mockClient.request(any(HttpRequest.class), eq(String.class)))
                    .thenThrow(
                        new HttpClientResponseException(
                            "Bad Gateway",
                            HttpResponse.<String> builder()
                                .status(HttpResponse.Status.builder().code(502).build())
                                .build()
                        )
                    )
                    .thenReturn(
                        HttpResponse.<String> builder()
                            .status(HttpResponse.Status.builder().code(200).build())
                            .body("{\"status\":\"ok\"}")
                            .build()
                    )
            )
        ) {

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
            verify(mockClient, times(2)).request(any(HttpRequest.class), eq(String.class));
        }
    }

    @Test
    void shouldNotRetryPostOnReadTimeout() throws Exception {
        // A read timeout on the non-idempotent POST /run/ is ambiguous — the run may already have been
        // created — so it must fail fast rather than risk starting the job twice.
        var runContext = runContextFactory.of(Map.of());
        var requestBuilder = HttpRequest.builder()
            .uri(new URI("https://fake.api/dbt"))
            .method("POST");

        try (
            var mocked = Mockito.mockConstruction(
                HttpClient.class,
                (mockClient, context) -> when(mockClient.request(any(HttpRequest.class), eq(String.class)))
                    .thenThrow(new RuntimeException(new SocketTimeoutException("Read timed out")))
            )
        ) {

            var task = CheckStatus.builder()
                .id(IdUtils.create())
                .type(CheckStatus.class.getName())
                .runId(Property.ofValue("123"))
                .token(Property.ofValue("fake-token"))
                .accountId(Property.ofValue("fake-account"))
                .maxRetries(Property.ofValue(3))
                .initialDelayMs(Property.ofValue(100L))
                .build();

            var ex = assertThrows(
                RuntimeException.class,
                () -> task.request(runContext, requestBuilder, Map.class)
            );
            assertInstanceOf(SocketTimeoutException.class, ex.getCause());

            var mockClient = mocked.constructed().getFirst();
            verify(mockClient, times(1)).request(any(HttpRequest.class), eq(String.class));
        }
    }

    @Test
    void shouldRetryOnReadTimeoutAndEventuallySucceed() throws Exception {
        // A transient read timeout during status polling is surfaced by the core HTTP client as
        // RuntimeException(SocketTimeoutException) and must be retried instead of failing the task
        // while the dbt Cloud run is still healthy.
        var runContext = runContextFactory.of(Map.of());
        var requestBuilder = HttpRequest.builder()
            .uri(new URI("https://fake.api/dbt"));

        try (
            var mocked = Mockito.mockConstruction(
                HttpClient.class,
                (mockClient, context) -> when(mockClient.request(any(HttpRequest.class), eq(String.class)))
                    .thenThrow(new RuntimeException(new SocketTimeoutException("Read timed out")))
                    .thenReturn(
                        HttpResponse.<String> builder()
                            .status(HttpResponse.Status.builder().code(200).build())
                            .body("{\"status\":\"ok\"}")
                            .build()
                    )
            )
        ) {

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
            verify(mockClient, times(2)).request(any(HttpRequest.class), eq(String.class));
        }
    }

    @Test
    void shouldNotRetryOnClientError() throws Exception {
        // A genuine client error (e.g. 404 for a wrong run id) must fail fast, not retry.
        var runContext = runContextFactory.of(Map.of());
        var requestBuilder = HttpRequest.builder()
            .uri(new URI("https://fake.api/dbt"));

        try (
            var mocked = Mockito.mockConstruction(
                HttpClient.class,
                (mockClient, context) -> when(mockClient.request(any(HttpRequest.class), eq(String.class)))
                    .thenThrow(
                        new HttpClientResponseException(
                            "Not Found",
                            HttpResponse.<String> builder()
                                .status(HttpResponse.Status.builder().code(404).build())
                                .build()
                        )
                    )
            )
        ) {

            var task = CheckStatus.builder()
                .id(IdUtils.create())
                .type(CheckStatus.class.getName())
                .runId(Property.ofValue("123"))
                .token(Property.ofValue("fake-token"))
                .accountId(Property.ofValue("fake-account"))
                .maxRetries(Property.ofValue(3))
                .initialDelayMs(Property.ofValue(100L))
                .build();

            var ex = assertThrows(
                HttpClientResponseException.class,
                () -> task.request(runContext, requestBuilder, Map.class)
            );
            assertEquals(404, ex.getResponse().getStatus().getCode());

            var mockClient = mocked.constructed().getFirst();
            verify(mockClient, times(1)).request(any(HttpRequest.class), eq(String.class));
        }
    }

    @Test
    void shouldFailAfterMaxRetries() throws Exception {
        var runContext = runContextFactory.of(Map.of());
        var requestBuilder = HttpRequest.builder()
            .uri(new URI("https://fake.api/dbt"));

        try (
            var mocked = Mockito.mockConstruction(
                HttpClient.class,
                (mockClient, context) -> when(mockClient.request(any(HttpRequest.class), eq(String.class)))
                    .thenThrow(
                        new HttpClientResponseException(
                            "Bad Gateway",
                            HttpResponse.<String> builder()
                                .status(HttpResponse.Status.builder().code(502).build())
                                .build()
                        )
                    )
                    .thenThrow(
                        new HttpClientResponseException(
                            "Bad Gateway",
                            HttpResponse.<String> builder()
                                .status(HttpResponse.Status.builder().code(502).build())
                                .build()
                        )
                    )
            )
        ) {

            var task = CheckStatus.builder()
                .id(IdUtils.create())
                .type(CheckStatus.class.getName())
                .runId(Property.ofValue("123"))
                .token(Property.ofValue("fake-token"))
                .accountId(Property.ofValue("fake-account"))
                .maxRetries(Property.ofValue(2))
                .initialDelayMs(Property.ofValue(100L))
                .build();

            var ex = assertThrows(
                RetryUtils.RetryFailed.class,
                () -> task.request(runContext, requestBuilder, Map.class)
            );

            assertInstanceOf(HttpClientResponseException.class, ex.getCause());
            var cause = (HttpClientResponseException) ex.getCause();
            assertEquals(502, cause.getResponse().getStatus().getCode());

            var mockClient = mocked.constructed().getFirst();
            verify(mockClient, times(2)).request(any(HttpRequest.class), eq(String.class));
        }
    }
}
