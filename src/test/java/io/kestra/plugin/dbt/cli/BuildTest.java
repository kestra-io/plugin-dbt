package io.kestra.plugin.dbt.cli;

import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.tasks.scripts.ScriptOutput;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

import static io.kestra.core.utils.Rethrow.throwConsumer;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertTrue;

@MicronautTest
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

        copyFolder(Path.of(Objects.requireNonNull(this.getClass().getClassLoader().getResource("project")).getPath()), runContext.tempDir(true));

        setup.run(runContext);

        Build task = Build.builder()
            .thread(8)
            .build();

        ScriptOutput runOutput = task.run(runContext);

        assertThat(runOutput.getExitCode(), is(0));
        assertTrue(runOutput.getOutputFiles().containsKey("run_results.json"));
        assertTrue(runOutput.getOutputFiles().containsKey("manifest.json"));
    }
}
