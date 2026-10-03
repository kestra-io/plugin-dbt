package io.kestra.plugin.dbt.cloud;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.kestra.core.http.client.configurations.HttpConfiguration;
import io.kestra.core.http.client.configurations.HttpMethod;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;

import jakarta.inject.Inject;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Retry classification (the old (Throwable, method) -> boolean predicate this file used to exercise via
 * {@code AbstractDbtCloud.isRetriableTransientError}) no longer exists as a standalone function: per-request
 * retry now happens entirely inside Core's {@code HttpClient}, driven by whatever {@code HttpConfiguration}
 * a caller supplies. That per-method/status-code behavior is covered directly in Core's own
 * {@code HttpClientTest} (see {@code shouldRetryPerMethodOverrideWhenMethodMatches} and neighbors).
 *
 * What's left for this plugin to own — and what the tests below check — is narrower: does
 * {@link AbstractDbtCloud#retryConfiguration} correctly translate {@code maxRetries}/{@code initialDelayMs}
 * and {@code excludeAmbiguousGatewayCodes} into the {@link HttpConfiguration} dbt Cloud actually needs (the
 * write/read-only status-code split, and the reattach-safe exclusion of 502/504).
 *
 * The old end-to-end tests here (mocking {@code HttpClient} entirely via {@code Mockito.mockConstruction}
 * to assert a throw-then-succeed retry sequence) are deliberately not carried forward: the real retry loop
 * now lives inside Core's {@code HttpClient.request()}, so mocking the whole class would mock away the very
 * thing under test. See the comment above {@code minimalCheckStatus()} for what a correct replacement needs.
 */
@KestraTest
class CheckStatusRetryTest {

    @Inject
    private RunContextFactory runContextFactory;

    // --- retryConfiguration() shape ---
    //
    // Property<T> only exposes its value through RunContext.render(...) (there is no static-inspection
    // accessor), so each test renders against a minimal RunContext, exactly as Core's own HttpClient does
    // internally in resolveRetryableStatusCodes()/isMethodEligibleForTransportRetry().

    @Test
    void retryConfigurationForNormalWriteKeepsAllFourGatewayStatusesRetriable() throws Exception {
        var task = minimalCheckStatus();
        var runContext = runContextFactory.of(Map.of());

        HttpConfiguration config = task.retryConfiguration(3, 100L, false);

        List<Integer> writeCodes = runContext.render(config.getRetryOnStatusCodes()).asList(Integer.class);
        assertTrue(writeCodes.containsAll(List.of(429, 502, 503, 504)));
    }

    @Test
    void retryConfigurationForReattachSafeCallExcludesAmbiguousGatewayCodes() throws Exception {
        var task = minimalCheckStatus();
        var runContext = runContextFactory.of(Map.of());

        HttpConfiguration config = task.retryConfiguration(3, 100L, true);

        List<Integer> writeCodes = runContext.render(config.getRetryOnStatusCodes()).asList(Integer.class);
        assertTrue(writeCodes.contains(429));
        assertTrue(writeCodes.contains(503));
        assertFalse(writeCodes.contains(502));
        assertFalse(writeCodes.contains(504));
    }

    @Test
    void retryConfigurationGivesGetAndHeadTheirOwnBroaderCodes() throws Exception {
        var task = minimalCheckStatus();
        var runContext = runContextFactory.of(Map.of());

        HttpConfiguration config = task.retryConfiguration(3, 100L, false);

        Map<HttpMethod, List<Integer>> byMethod = runContext
            .render(config.getRetryOnStatusCodesByMethod())
            .asMap(HttpMethod.class, List.class);

        assertTrue(byMethod.get(HttpMethod.GET).containsAll(List.of(429, 500, 501, 599)));
        assertTrue(byMethod.get(HttpMethod.HEAD).containsAll(List.of(429, 500, 501, 599)));
        assertFalse(byMethod.containsKey(HttpMethod.POST));
    }

    @Test
    void retryConfigurationLimitsTransportFailureRetryToGetAndHead() throws Exception {
        var task = minimalCheckStatus();
        var runContext = runContextFactory.of(Map.of());

        HttpConfiguration config = task.retryConfiguration(3, 100L, false);

        List<HttpMethod> transportRetriable = runContext
            .render(config.getRetryableTransportFailureMethods())
            .asList(HttpMethod.class);

        assertEquals(List.of(HttpMethod.GET, HttpMethod.HEAD), transportRetriable);
    }

    @Test
    void retryConfigurationTranslatesMaxRetriesAndInitialDelayIntoExponentialPolicy() {
        var task = minimalCheckStatus();

        HttpConfiguration config = task.retryConfiguration(5, 250L, false);

        assertInstanceOf(io.kestra.core.models.tasks.retrys.Exponential.class, config.getRetry());
        var exponential = (io.kestra.core.models.tasks.retrys.Exponential) config.getRetry();
        assertEquals(5, exponential.getMaxAttempts());
        assertEquals(java.time.Duration.ofMillis(250L), exponential.getInterval());
    }

    // --- end-to-end retry-loop coverage: currently a documented gap, not a test ---
    //
    // The two tests this file used to have here (shouldRetryReadOnServerErrorAndEventuallySucceed,
    // shouldFailAfterMaxRetries) mocked HttpClient entirely via Mockito.mockConstruction. That worked
    // before this migration because AbstractDbtCloud owned the retry loop itself (calling client.request()
    // repeatedly via its own RetryUtils.of(...).run(...)), so mocking HttpClient and stubbing a
    // throw-then-succeed sequence exercised AbstractDbtCloud's loop correctly.
    //
    // That loop has moved: it now lives inside Core's real HttpClient.request(), wrapped around
    // configuration.getRetry(). Mocking HttpClient's construction bypasses that real method entirely, so
    // the mocked-out versions of these two tests would now pass or fail for the wrong reason (a single
    // stubbed call, not an actual retry), regardless of whether AbstractDbtCloud.retryConfiguration()
    // is correct.
    //
    // I'm intentionally not shipping a fake replacement here rather than guess at plugin-dbt's test
    // infrastructure. A correct version needs either:
    //   (a) a real embedded test server that requests actually hit, the way Core's own HttpClientTest
    //       does it (see ClientTestController + EmbeddedServer in core's test sourceset) — the request
    //       fails N times then succeeds, and a real HttpClient/HttpConfiguration built by
    //       AbstractDbtCloud.retryConfiguration() is exercised against it end to end, or
    //   (b) mocking only the underlying Apache HttpClient5 CloseableHttpClient that a real
    //       io.kestra.core.http.client.HttpClient wraps, not the wrapper itself.
    // I don't know whether plugin-dbt's test module already has an embedded-server harness (Core's lives
    // in core's own test sourceset, and I haven't seen plugin-dbt's build.gradle/test dependencies) or
    // whether Apache HttpClient5 test doubles are already on this module's test classpath. Please wire
    // whichever fits what's already available here — the retryConfiguration() unit tests above cover the
    // logic this plugin is actually responsible for in the meantime, and Core's HttpClientTest already
    // covers the generic per-method retry loop itself.

    private CheckStatus minimalCheckStatus() {
        return CheckStatus.builder()
            .id(IdUtils.create())
            .type(CheckStatus.class.getName())
            .runId(Property.ofValue("123"))
            .token(Property.ofValue("fake-token"))
            .accountId(Property.ofValue("fake-account"))
            .build();
    }
}