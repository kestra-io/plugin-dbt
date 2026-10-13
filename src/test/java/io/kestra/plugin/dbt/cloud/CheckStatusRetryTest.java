package io.kestra.plugin.dbt.cloud;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.util.Map;
import java.net.SocketException;

import javax.net.ssl.SSLHandshakeException;

import org.junit.jupiter.api.Test;

import io.kestra.core.http.client.configurations.HttpConfiguration;
import io.kestra.core.http.client.configurations.TimeoutConfiguration;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.retrys.Constant;
import io.kestra.core.models.tasks.retrys.Exponential;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;

import jakarta.inject.Inject;

import static org.junit.jupiter.api.Assertions.*;

@KestraTest
class CheckStatusRetryTest {

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void retryConfigurationForNormalWriteKeepsAllFourGatewayStatusesRetriable() throws Exception {
        var runContext = runContextFactory.of(Map.of());

        var config = checkStatus(null).retryConfiguration(3, 100L, false);

        var writeCodes = runContext.render(config.getRetryOnStatusCodes()).asList(Integer.class);
        assertTrue(writeCodes.containsAll(List.of(429, 502, 503, 504)));
    }

    @Test
    void retryConfigurationForReattachSafeCallExcludesAmbiguousGatewayCodes() throws Exception {
        var runContext = runContextFactory.of(Map.of());

        var config = checkStatus(null).retryConfiguration(3, 100L, true);

        var writeCodes = runContext.render(config.getRetryOnStatusCodes()).asList(Integer.class);
        assertTrue(writeCodes.contains(429));
        assertTrue(writeCodes.contains(503));
        assertFalse(writeCodes.contains(502));
        assertFalse(writeCodes.contains(504));
    }

    @Test
    void retryConfigurationGivesGetAndHeadTheirOwnBroaderCodes() throws Exception {
        var runContext = runContextFactory.of(Map.of());

        var config = checkStatus(null).retryConfiguration(3, 100L, false);

        var byMethod = runContext
            .render(config.getRetryOnStatusCodesByMethod())
            .asMap(String.class, List.class);

        assertTrue(byMethod.get("GET").containsAll(List.of(429, 500, 501, 599)));
        assertTrue(byMethod.get("HEAD").containsAll(List.of(429, 500, 501, 599)));
        // Core only retries a non-idempotent method on status codes when it is listed here.
        assertTrue(byMethod.get("POST").containsAll(List.of(429, 502, 503, 504)));
        assertFalse(byMethod.get("POST").contains(500));
    }

    @Test
    void retryConfigurationReattachKeepsPostOffAmbiguousGatewayCodes() throws Exception {
        var runContext = runContextFactory.of(Map.of());

        var config = checkStatus(null).retryConfiguration(3, 100L, true);

        var post = runContext
            .render(config.getRetryOnStatusCodesByMethod())
            .asMap(String.class, List.class)
            .get("POST");
        assertTrue(post.containsAll(List.of(429, 503)));
        assertFalse(post.contains(502) || post.contains(504));
    }

    @Test
    void retryConfigurationLimitsTransportFailureRetryToGetAndHead() throws Exception {
        var runContext = runContextFactory.of(Map.of());

        var config = checkStatus(null).retryConfiguration(3, 100L, false);

        var transportRetriable = runContext
            .render(config.getRetryableTransportFailureMethods())
            .asList(String.class);

        assertEquals(List.of("GET", "HEAD"), transportRetriable);
    }

    @Test
    void retryConfigurationTranslatesMaxRetriesAndInitialDelayIntoExponentialPolicy() {
        var config = checkStatus(null).retryConfiguration(5, 250L, false);

        var exponential = assertInstanceOf(Exponential.class, config.getRetry());
        assertEquals(5, exponential.getMaxAttempts());
        assertEquals(Duration.ofMillis(250L), exponential.getInterval());
    }

    @Test
    void retryConfigurationAppliesDeprecatedPropertiesWhenOptionsHasNoRetry() {
        var options = HttpConfiguration.builder()
            .timeout(TimeoutConfiguration.builder()
                .readIdleTimeout(Property.ofValue(Duration.ofSeconds(5)))
                .build())
            .build();

        var config = checkStatus(options).retryConfiguration(4, 200L, false);

        var exponential = assertInstanceOf(Exponential.class, config.getRetry());
        assertEquals(4, exponential.getMaxAttempts());
        assertNotNull(config.getTimeout());
    }

    @Test
    void retryConfigurationKeepsUserSetOptionsRetry() {
        var userRetry = Constant.builder()
            .interval(Duration.ofMillis(10))
            .maxAttempts(7)
            .build();
        var options = HttpConfiguration.builder().retry(userRetry).build();

        var config = checkStatus(options).retryConfiguration(3, 100L, false);

        assertSame(userRetry, config.getRetry());
    }

    @Test
    void retryConfigurationKeepsUserSetStatusCodesAndTransportMethods() throws Exception {
        var runContext = runContextFactory.of(Map.of());
        var options = HttpConfiguration.builder()
            .retry(Constant.builder().interval(Duration.ofMillis(10)).maxAttempts(2).build())
            .retryOnStatusCodes(Property.ofValue(List.of(418)))
            .retryableTransportFailureMethods(Property.ofValue(List.of("GET", "POST")))
            .build();

        var config = checkStatus(options).retryConfiguration(3, 100L, true);

        assertEquals(List.of(418), runContext.render(config.getRetryOnStatusCodes()).asList(Integer.class));
        assertEquals(
            List.of("GET", "POST"),
            runContext.render(config.getRetryableTransportFailureMethods()).asList(String.class)
        );
        // Not set by the user, so it still gets the plugin default.
        assertNotNull(config.getRetryOnStatusCodesByMethod());
    }

    @Test
    void emptyResponseBodyIsATransientReadFailure() {
        assertTrue(CheckStatus.isTransientReadFailure(new IOException("Empty response body from dbt Cloud")));
    }

    @Test
    void transportFailuresAreTransientReadFailures() {
        assertTrue(CheckStatus.isTransientReadFailure(new SSLHandshakeException("handshake")));
        assertTrue(CheckStatus.isTransientReadFailure(new IOException("connection reset", new SocketException())));
    }

    @Test
    void nonTransportFailuresAreNotTransientReadFailures() {
        assertFalse(CheckStatus.isTransientReadFailure(new IllegalArgumentException("bad config")));
        assertFalse(CheckStatus.isTransientReadFailure(null));
    }

    private CheckStatus checkStatus(HttpConfiguration options) {
        return CheckStatus.builder()
            .id(IdUtils.create())
            .type(CheckStatus.class.getName())
            .runId(Property.ofValue("123"))
            .token(Property.ofValue("fake-token"))
            .accountId(Property.ofValue("fake-account"))
            .options(options)
            .build();
    }

    @Test
    void isInterruption_detectsDirectWrappedAndFlaggedInterrupts() {
        assertTrue(CheckStatus.isInterruption(new InterruptedException()));
        assertTrue(CheckStatus.isInterruption(new RuntimeException(new IOException(new InterruptedException()))));
        assertFalse(CheckStatus.isInterruption(status(502)));
        assertFalse(CheckStatus.isInterruption(new IOException("Empty response body from dbt Cloud")));

        Thread.currentThread().interrupt();
        try {
            assertTrue(CheckStatus.isInterruption(status(502)));
        } finally {
            Thread.interrupted();
        }
    }

    private static IOException status(int code) {
        return new IOException("dbt Cloud responded with HTTP " + code);
    }
}
