package io.kestra.plugin.dbt.cli;

import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.core.runner.Process;
import io.kestra.plugin.scripts.exec.scripts.models.ScriptOutput;
import io.kestra.core.junit.annotations.KestraTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.List;
import java.util.stream.Stream;

import static io.kestra.core.utils.Rethrow.throwConsumer;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertTrue;

@KestraTest
class BuildTest {
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
            .taskRunner(Process.INSTANCE)
            .profiles(Map.of(
                "unit-kestra", Map.of(
                    "outputs", Map.of(
                            "dev", Map.of("dataset", "kestra_unit_test_us",
                            "fixed_retries", "1",
                            "location", "US",
                            "method", "oauth",
                            "priority", "interactive",
                            "project", "kestra-unit-test",
                            "threads", "1",
                            "timeout_seconds", "300",
                            "type", "bigquery"
                        )
                    ),
                    "target", "dev"
                )))
            .requirements(List.of("dbt-bigquery"))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, setup, Map.of());

        copyFolder(Path.of(Objects.requireNonNull(this.getClass().getClassLoader().getResource("project")).getPath()), runContext.workingDir().path(true));

        setup.run(runContext);

        try(var inputStream = new ByteArrayInputStream(Base64.getDecoder().decode(System.getenv("GOOGLE_SERVICE_ACCOUNT").getBytes()))) {
            Files.copy(inputStream, runContext.workingDir().resolve(Path.of("sa.json")));
        }
        Map<String, String> env = new HashMap<>();
        env.put("GOOGLE_APPLICATION_CREDENTIALS", runContext.workingDir().resolve(Path.of("sa.json")).toString());
        Build task = Build.builder()
            .thread(8)
            .env(env)
            .build();

        ScriptOutput runOutput = task.run(runContext);

        assertThat(runOutput.getExitCode(), is(0));
        assertTrue(runOutput.getOutputFiles().containsKey("run_results.json"));
        assertTrue(runOutput.getOutputFiles().containsKey("manifest.json"));
        assertThat(runContext.dynamicWorkerResults(), hasSize(13));
    }
}
