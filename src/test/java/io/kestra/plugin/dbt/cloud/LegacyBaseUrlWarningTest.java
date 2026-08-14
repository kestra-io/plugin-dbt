package io.kestra.plugin.dbt.cloud;

import java.net.URI;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import io.kestra.core.http.HttpRequest;
import io.kestra.core.http.HttpResponse;
import io.kestra.core.http.client.HttpClient;
import io.kestra.core.http.client.HttpClientResponseException;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

/**
 * Regression coverage for issue #317: a 401 against the legacy default baseUrl
 * (https://cloud.getdbt.com) must fail with a message pointing at baseUrl instead of only
 * suggesting a bad token, since that host no longer resolves tokens for regional and
 * cell-based dbt Cloud accounts.
 */
@KestraTest
class LegacyBaseUrlWarningTest {

    @Inject
    private RunContextFactory runContextFactory;

    private static HttpClientResponseException unauthorized() {
        return new HttpClientResponseException(
            "status 401",
            HttpResponse.<String> builder()
                .status(HttpResponse.Status.builder().code(401).build())
                .build()
        );
    }

    @Test
    void shouldSurfaceBaseUrlGuidanceOn401AgainstLegacyDefault() throws Exception {
        var task = CheckStatus.builder()
            .id(IdUtils.create())
            .type(CheckStatus.class.getName())
            .runId(Property.ofValue("123"))
            .token(Property.ofValue("fake-token"))
            .accountId(Property.ofValue("fake-account"))
            // baseUrl left unset: resolves to the legacy default https://cloud.getdbt.com
            .maxRetries(Property.ofValue(1))
            .initialDelayMs(Property.ofValue(10L))
            .build();

        var runContext = mockRunContext(task);
        var requestBuilder = HttpRequest.builder()
            .uri(new URI("https://cloud.getdbt.com/api/v2/accounts/fake-account/runs/123/"))
            .method("GET");

        try (
            var mocked = Mockito.mockConstruction(
                HttpClient.class,
                (mockClient, context) -> when(mockClient.request(any(HttpRequest.class), eq(String.class)))
                    .thenThrow(unauthorized())
            )
        ) {
            var ex = assertThrows(
                HttpClientResponseException.class,
                () -> task.request(runContext, requestBuilder, Object.class)
            );

            // The 401 still fails the task, but now names baseUrl as the likely cause.
            assertThat(ex.getResponse().getStatus().getCode(), is(401));
            assertThat(ex.getMessage(), containsString("Received a 401"));
            assertThat(ex.getMessage(), containsString("baseUrl"));
        }
    }

    @Test
    void shouldNotAddBaseUrlGuidanceOn401AgainstCustomBaseUrl() throws Exception {
        var task = CheckStatus.builder()
            .id(IdUtils.create())
            .type(CheckStatus.class.getName())
            .baseUrl(Property.ofValue("https://my-account.us1.dbt.com"))
            .runId(Property.ofValue("123"))
            .token(Property.ofValue("fake-token"))
            .accountId(Property.ofValue("fake-account"))
            .maxRetries(Property.ofValue(1))
            .initialDelayMs(Property.ofValue(10L))
            .build();

        var runContext = mockRunContext(task);
        var requestBuilder = HttpRequest.builder()
            .uri(new URI("https://my-account.us1.dbt.com/api/v2/accounts/fake-account/runs/123/"))
            .method("GET");

        try (
            var mocked = Mockito.mockConstruction(
                HttpClient.class,
                (mockClient, context) -> when(mockClient.request(any(HttpRequest.class), eq(String.class)))
                    .thenThrow(unauthorized())
            )
        ) {
            var ex = assertThrows(
                HttpClientResponseException.class,
                () -> task.request(runContext, requestBuilder, Object.class)
            );

            // A 401 against a custom baseUrl is left untouched: the original error passes through.
            assertThat(ex.getResponse().getStatus().getCode(), is(401));
            assertThat(ex.getMessage(), not(containsString("baseUrl")));
            assertThat(ex.getMessage(), not(containsString("Received a 401")));
        }
    }

    private RunContext mockRunContext(CheckStatus task) {
        var flow = TestsUtils.mockFlow();
        var execution = TestsUtils.mockExecution(flow, java.util.Map.of(), null);
        var taskRun = TestsUtils.mockTaskRun(execution, task);
        return runContextFactory.of(flow, task, execution, taskRun, false);
    }
}
