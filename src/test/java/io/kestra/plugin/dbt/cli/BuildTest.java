package io.kestra.plugin.dbt.cli;

import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.executions.TaskRun;
import io.kestra.core.models.flows.Flow;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.core.runner.Process;
import io.kestra.plugin.scripts.exec.scripts.models.ScriptOutput;
import io.kestra.core.junit.annotations.KestraTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static io.kestra.core.utils.Rethrow.throwConsumer;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertTrue;

@KestraTest
class BuildTest {
    private static final String PROFILES = """
                    unit-kestra:
                      outputs:
                        dev:
                          dataset: kestra_unit_test_us
                          fixed_retries: 1
                          location: US
                          method: service-account
                          priority: interactive
                          project: kestra-unit-test
                          threads: 1
                          timeout_seconds: 300
                          type: bigquery
                          keyfile: sa.json
                      target: dev
                    """;
    @Inject
    private RunContextFactory runContextFactory;

    public void copyFolder(Path src, Path dest) throws IOException {
        try (Stream<Path> stream = Files.walk(src)) {
            stream
                .forEach(throwConsumer(source -> Files.copy(
                    source,
                    dest.resolve(src.relativize(source)),
                    REPLACE_EXISTING
                )));
        }
    }

    @Test
    void run() throws Exception {
        Setup setup = Setup.builder()
            .id(IdUtils.create())
            .type(Setup.class.getName())
            .taskRunner(Process.instance())
            .profiles(Property.ofValue(PROFILES))
            .requirements(Property.ofValue(List.of(
                "dbt-bigquery==1.8.3",
                "click\\<8.1"
            )))
            .build();

        RunContext runContext = mockRunContext(setup);

        copyFolder(Path.of(Objects.requireNonNull(this.getClass().getClassLoader().getResource("project")).getPath()), runContext.workingDir().path(true));

        // Copy sa.json before to run the task
        String encodedServiceAccount = System.getenv("GOOGLE_SERVICE_ACCOUNT");
        Assumptions.assumeTrue(encodedServiceAccount != null && !encodedServiceAccount.isBlank(), "GOOGLE_SERVICE_ACCOUNT is not set");
        try (var inputStream = new ByteArrayInputStream(Base64.getDecoder().decode(encodedServiceAccount.getBytes()))) {
            Files.copy(inputStream, runContext.workingDir().resolve(Path.of("sa.json")));
        }

        Map<String, String> env = new HashMap<>();
        env.put("GOOGLE_APPLICATION_CREDENTIALS", runContext.workingDir().resolve(Path.of("sa.json")).toString());

        setup.run(runContext);

        Build task = Build.builder()
            .thread(Property.ofValue(8))
            .taskRunner(Process.instance())
            .env(Property.ofValue(env))
            .build();

        ScriptOutput runOutput = task.run(runContext);

        assertThat(runOutput.getExitCode(), is(0));
        assertTrue(runOutput.getOutputFiles().containsKey("run_results.json"));
        assertTrue(runOutput.getOutputFiles().containsKey("manifest.json"));
        assertThat(runContext.dynamicWorkerResults(), hasSize(12));
    }

    private RunContext mockRunContext(Task task) {
        ensureFactorySecretKey();
        Flow flow = TestsUtils.mockFlow();
        Execution execution = TestsUtils.mockExecution(flow, Map.of(), null);
        TaskRun taskRun = TestsUtils.mockTaskRun(execution, task);
        RunContext runContext = runContextFactory.of(flow, task, execution, taskRun, false);
        ensureSecretKey(runContext);
        return runContext;
    }

    private void ensureFactorySecretKey() {
        try {
            Class<?> type = runContextFactory.getClass();
            java.lang.reflect.Field field = null;
            while (type != null && field == null) {
                try {
                    field = type.getDeclaredField("secretKey");
                } catch (NoSuchFieldException e) {
                    type = type.getSuperclass();
                }
            }
            if (field == null) {
                return;
            }
            field.setAccessible(true);
            if (field.get(runContextFactory) == null) {
                field.set(runContextFactory, Optional.empty());
            }
        } catch (IllegalAccessException e) {
            throw new IllegalStateException("Failed to initialize run context factory secret key", e);
        }
    }

    private void ensureSecretKey(RunContext runContext) {
        try {
            Class<?> type = runContext.getClass();
            java.lang.reflect.Field field = null;
            while (type != null && field == null) {
                try {
                    field = type.getDeclaredField("secretKey");
                } catch (NoSuchFieldException e) {
                    type = type.getSuperclass();
                }
            }
            if (field == null) {
                return;
            }
            field.setAccessible(true);
            if (field.get(runContext) == null) {
                field.set(runContext, Optional.empty());
            }
        } catch (IllegalAccessException e) {
            throw new IllegalStateException("Failed to initialize run context secret key", e);
        }
    }
}
