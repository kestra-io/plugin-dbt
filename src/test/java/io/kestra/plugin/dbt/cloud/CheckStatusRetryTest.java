package io.kestra.plugin.dbt.cloud;

import java.net.SocketTimeoutException;
import java.net.URI;
import java.util.Map;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

import io.kestra.core.http.HttpRequest;
import io.kestra.core.http.HttpResponse;
import io.kestra.core.http.client.HttpClient;
import io.kestra.core.http.client.HttpClientRequestException;
import io.kestra.core.http.client.HttpClientResponseException;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.RetryUtils;

import jakarta.inject.Inject;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@KestraTest
class CheckStatusRetryTest {

    @Inject
    private RunContextFactory runContextFactory;

    private static HttpClientResponseException status(int code) {
        return new HttpClientResponseException(
            "status " + code,
            HttpResponse.<String> builder()
                .status(HttpResponse.Status.builder().code(code).build())
                .build()
        );
    }

    private static HttpClientRequestException connectionFailure() {
        return new HttpClientRequestException(
            "connection failed",
            HttpRequest.builder().uri(URI.create("https://fake.api/dbt")).build()
        );
    }

    static Stream<Arguments> retryCases() {
        return Stream.of(
            // Null throwable is never retried.
            arguments((Throwable) null, "GET", false),

            // Read-only methods retry every 5xx, connection failures and timeouts.
            arguments(status(500), "GET", true),
            arguments(status(501), "GET", true),
            arguments(status(503), "GET", true),
            arguments(status(599), "GET", true),
            arguments(status(500), "HEAD", true),
            arguments(status(500), "get", true), // case-insensitive
            arguments(connectionFailure(), "GET", true),
            arguments(new SocketTimeoutException("read timed out"), "GET", true),
            arguments(new RuntimeException(new SocketTimeoutException("read timed out")), "GET", true),

            // Read-only client errors (other than 429) are not retried.
            arguments(status(400), "GET", false),
            arguments(status(404), "GET", false),

            // Write methods retry only 502/503/504; a 500, timeout or connection failure fails fast.
            arguments(status(502), "POST", true),
            arguments(status(503), "POST", true),
            arguments(status(504), "POST", true),
            arguments(status(500), "POST", false),
            arguments(status(501), "POST", false),
            arguments(connectionFailure(), "POST", false),
            arguments(new RuntimeException(new SocketTimeoutException("read timed out")), "POST", false),

            // PUT/DELETE are RFC-idempotent but treated as write methods here (not blindly retriable).
            arguments(status(503), "PUT", true),
            arguments(status(500), "PUT", false),
            arguments(status(503), "DELETE", true),

            // A null/unknown method is treated as a write method (conservative default).
            arguments(status(503), null, true),
            arguments(status(500), null, false),

            // 429 (rate limited) is retried for any method.
            arguments(status(429), "GET", true),
            arguments(status(429), "POST", true)
        );
    }

    @ParameterizedTest(name = "[{index}] {1} {0} -> retriable={2}")
    @MethodSource("retryCases")
    void isRetriableTransientError_matrix(Throwable throwable, String method, boolean expected) {
        assertEquals(expected, AbstractDbtCloud.isRetriableTransientError(throwable, method));
    }

    @Test
    void shouldRetryReadOnServerErrorAndEventuallySucceed() throws Exception {
        // End-to-end wiring: a transient 500 during status polling (an idempotent GET) is retried
        // through RetryUtils instead of failing the task while the dbt Cloud run is still healthy.
        var runContext = runContextFactory.of(Map.of());
        var requestBuilder = HttpRequest.builder()
            .uri(new URI("https://fake.api/dbt"))
            .method("GET");

        try (
            var mocked = Mockito.mockConstruction(
                HttpClient.class,
                (mockClient, context) -> when(mockClient.request(any(HttpRequest.class), eq(String.class)))
                    .thenThrow(status(500))
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
    void shouldFailAfterMaxRetries() throws Exception {
        // End-to-end wiring: once retries are exhausted, RetryUtils surfaces the last error as RetryFailed.
        var runContext = runContextFactory.of(Map.of());
        var requestBuilder = HttpRequest.builder()
            .uri(new URI("https://fake.api/dbt"))
            .method("GET");

        try (
            var mocked = Mockito.mockConstruction(
                HttpClient.class,
                (mockClient, context) -> when(mockClient.request(any(HttpRequest.class), eq(String.class)))
                    .thenThrow(status(502))
                    .thenThrow(status(502))
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
