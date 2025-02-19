package io.kestra.plugin.dbt.cloud;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.flows.State;
import io.kestra.core.queues.QueueException;
import io.kestra.core.repositories.LocalFlowRepositoryLoader;

import io.kestra.core.runners.RunnerUtils;
import io.kestra.core.runners.StandAloneRunner;
import io.micronaut.context.ApplicationContext;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Post;
import io.micronaut.runtime.server.EmbeddedServer;
import jakarta.inject.Inject;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
class SerializationTest {
    @Inject
    private ApplicationContext applicationContext;

    @Inject
    protected StandAloneRunner runner;

    @Inject
    protected RunnerUtils runnerUtils;

    @Inject
    protected LocalFlowRepositoryLoader repositoryLoader;

    @BeforeEach
    protected void init() throws IOException, URISyntaxException {
        repositoryLoader.load(Objects.requireNonNull(SerializationTest.class.getClassLoader().getResource("flows")));
        this.runner.run();
    }

    @Test
    void flow() throws TimeoutException, QueueException {
        EmbeddedServer embeddedServer = applicationContext.getBean(EmbeddedServer.class);
        embeddedServer.start();

        Execution execution = runnerUtils.runOne(
            null,
            "io.kestra.tests",
            "cloud",
            null,
            (f, e) -> ImmutableMap.of("url", embeddedServer.getURI().toString()),
            Duration.ofMinutes(10)
        );

        assertThat(execution.getState().getCurrent(), is(State.Type.SUCCESS));
    }

    @Controller()
    public static class FakeDbtCloudController {
        @Post("/api/v2/accounts/{accountId}/jobs/{jobId}/run")
        public HttpResponse<String> get(String jobId) throws IOException {
            return HttpResponse.ok(IOUtils.toString(Objects.requireNonNull(SerializationTest.class.getClassLoader().getResourceAsStream("responses/run.json")), StandardCharsets.UTF_8));
        }
    }
}
