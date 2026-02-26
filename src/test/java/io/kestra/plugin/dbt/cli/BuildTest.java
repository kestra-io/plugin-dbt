package io.kestra.plugin.dbt.cli;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.core.runner.Process;
import io.kestra.plugin.scripts.exec.scripts.models.ScriptOutput;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.List;
import java.util.stream.Stream;

import static io.kestra.core.utils.Rethrow.throwConsumer;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
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
        var pythonPath = resolvePythonPath();
        Assumptions.assumeTrue(pythonPath != null, "python or python3 is not available");
        Assumptions.assumeTrue(isDbtCompatiblePython(pythonPath), "dbt-bigquery==1.8.3 requires Python <= 3.12");
        Assumptions.assumeTrue(isPypiReachable(), "pypi.org is not reachable");

        Setup setup = Setup.builder()
            .id(IdUtils.create())
            .type(Setup.class.getName())
            .taskRunner(Process.instance())
            .pythonPath(pythonPath)
            .profiles(Property.ofValue(PROFILES))
            .requirements(Property.ofValue(List.of(
                "dbt-bigquery==1.8.3",
                "click\\<8.1"
            )))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, setup, Map.of());

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

    private String resolvePythonPath() {
        var path = System.getenv("PATH");
        if (path == null || path.isBlank()) {
            return null;
        }

        for (var candidate : List.of("python", "python3")) {
            var exists = Arrays.stream(path.split(File.pathSeparator))
                .map(Path::of)
                .map(dir -> dir.resolve(candidate))
                .anyMatch(Files::isExecutable);
            if (exists) {
                return candidate;
            }
        }

        return null;
    }

    private boolean isPypiReachable() {
        try (var socket = new Socket()) {
            socket.connect(new InetSocketAddress("pypi.org", 443), 2_000);
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    private boolean isDbtCompatiblePython(String pythonPath) {
        try {
            var process = new ProcessBuilder(
                pythonPath,
                "-c",
                "import sys; print(f\"{sys.version_info.major}.{sys.version_info.minor}\")"
            ).start();

            try (var reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
                var version = reader.readLine();
                if (version == null || process.waitFor() != 0) {
                    return false;
                }
                var parts = version.trim().split("\\.");
                if (parts.length != 2) {
                    return false;
                }

                var major = Integer.parseInt(parts[0]);
                var minor = Integer.parseInt(parts[1]);
                return major == 3 && minor <= 12;
            }
        } catch (IOException | InterruptedException | NumberFormatException e) {
            return false;
        }
    }
}
