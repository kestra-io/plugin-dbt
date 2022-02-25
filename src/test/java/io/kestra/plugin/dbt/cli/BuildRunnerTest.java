package io.kestra.plugin.dbt.cli;

import io.kestra.core.models.flows.State;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.repositories.LocalFlowRepositoryLoader;
import io.kestra.core.runners.RunnerUtils;
import io.kestra.runner.memory.MemoryRunner;

import jakarta.inject.Inject;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@MicronautTest
class BuildRunnerTest {
    @Inject
    protected MemoryRunner runner;

    @Inject
    protected RunnerUtils runnerUtils;

    @Inject
    protected LocalFlowRepositoryLoader repositoryLoader;

    @BeforeEach
    private void init() throws IOException, URISyntaxException {
        repositoryLoader.load(Objects.requireNonNull(BuildRunnerTest.class.getClassLoader().getResource("flows")));
        this.runner.run();
    }

    @Test
    void flow() throws TimeoutException, URISyntaxException {
        Path project = Path.of(Objects.requireNonNull(BuildRunnerTest.class.getClassLoader().getResource("project")).toURI());
        Execution execution = runnerUtils.runOne("io.kestra.dbt", "full", null, (flow, execution1) -> Map.of("path", project.toAbsolutePath().toUri().getPath()), Duration.ofMinutes(1));

        assertThat(execution.getState().getCurrent(), is(State.Type.SUCCESS));
        assertThat(execution.getTaskRunList().size(), greaterThan(10));
        assertThat(execution.findTaskRunsByTaskId("run").get(0).getState().getCurrent(), is(State.Type.SUCCESS));
        assertThat(execution.getTaskRunList().stream().filter(r -> r.getParentTaskRunId() != null && r.getParentTaskRunId().equals(execution.findTaskRunsByTaskId("run").get(0).getId())).count(), greaterThan(0L));
    }
}
