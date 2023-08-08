package io.kestra.plugin.dbt.cli;

import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.scripts.exec.scripts.models.DockerOptions;
import io.kestra.plugin.scripts.exec.scripts.models.ScriptOutput;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.io.File;
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

@MicronautTest
class DbtCLITest {
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
        DbtCLI execute = DbtCLI.builder()
            .id(IdUtils.create())
            .type(DbtCLI.class.getName())
            .profiles("""
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
                    """
            )
            .docker(DockerOptions.builder()
                .image("ghcr.io/dbt-labs/dbt-bigquery")
                .entryPoint(List.of())
                .build()
            )
            .commands(List.of("dbt build"))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, execute, Map.of());

        Path workingDir = runContext.tempDir(true);
        copyFolder(Path.of(Objects.requireNonNull(this.getClass().getClassLoader().getResource("project")).getPath()), workingDir);
        createSaFile(workingDir);

        ScriptOutput runOutput = execute.run(runContext);

        assertThat(runOutput.getExitCode(), is(0));
    }

    private void createSaFile(Path workingDir) throws IOException {
        Path existingSa = Path.of(System.getenv("GOOGLE_APPLICATION_CREDENTIALS"));
        Path workingDirSa = workingDir.resolve("sa.json");
        Files.copy(existingSa, workingDirSa);
    }
}
