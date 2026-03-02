package io.kestra.plugin.dbt.cli;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.flows.State;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTaskException;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.kv.KVStore;
import io.kestra.core.storages.kv.KVValueAndMetadata;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.core.runner.Process;
import io.kestra.plugin.scripts.exec.scripts.models.ScriptOutput;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

import static io.kestra.core.utils.Rethrow.throwConsumer;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
class DbtCLITest {
    @Inject
    private RunContextFactory runContextFactory;

    private static final String FLOW_NAMESPACE = "{{ flow.namespace }}";

    private static final String MANIFEST_KEY = "manifest.json";

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

    @ParameterizedTest
    @EnumSource(DbtCLI.LogFormat.class)
    void run(DbtCLI.LogFormat logFormat) throws Exception {
        DbtCLI execute = DbtCLI.builder()
            .id(IdUtils.create())
            .type(DbtCLI.class.getName())
            .profiles(Property.ofValue(PROFILES)
            )
            .logFormat(Property.ofValue(logFormat))
            .containerImage(Property.ofValue("ghcr.io/kestra-io/dbt-bigquery:latest"))
            .commands(Property.ofValue(List.of("dbt build --select zipcode")))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, execute, Map.of());

        Path workingDir = runContext.workingDir().path(true);
        copyFolder(Path.of(Objects.requireNonNull(this.getClass().getClassLoader().getResource("project")).getPath()), workingDir);
        createSaFile(workingDir);

        ScriptOutput runOutput = execute.run(runContext);

        assertThat(runOutput.getExitCode(), is(0));
    }

    @Test
    void testDbtCliWithStoreManifest_manifestShouldBePresentInKvStore() throws Exception {
        DbtCLI execute = DbtCLI.builder()
            .id(IdUtils.create())
            .type(DbtCLI.class.getName())
            .profiles(Property.ofValue(PROFILES)
            )
            .containerImage(Property.ofValue("ghcr.io/kestra-io/dbt-bigquery:latest"))
            .commands(Property.ofValue(List.of("dbt build")))
            .storeManifest(
                DbtCLI.KvStoreManifest.builder()
                    .key(Property.ofValue(MANIFEST_KEY))
                    .namespace(new Property<>(FLOW_NAMESPACE))
                    .build()
            )
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, execute, Map.of());

        Path workingDir = runContext.workingDir().path(true);
        copyFolder(Path.of(Objects.requireNonNull(this.getClass().getClassLoader().getResource("project")).getPath()), workingDir);
        createSaFile(workingDir);

        ScriptOutput runOutput = execute.run(runContext);

        assertThat(runOutput.getExitCode(), is(0));
        var namespace = runContext.render(FLOW_NAMESPACE);
        KVStore kvStore = runContext.namespaceKv(namespace);
        assertThat(kvStore.get(MANIFEST_KEY).isPresent(), is(true));
        Map<String, Object> manifestValue = (Map<String, Object>) kvStore.getValue(MANIFEST_KEY).get().value();
        assertThat(((Map<String, Object>) manifestValue.get("metadata")).get("project_name"), is("unit_kestra"));
    }

    @Test
    void testDbtWithLoadManifest_manifestShouldBeLoadedFromKvStore() throws Exception {
        DbtCLI loadManifest = DbtCLI.builder()
            .id(IdUtils.create())
            .type(DbtCLI.class.getName())
            .profiles(Property.ofValue(PROFILES))
            .projectDir(Property.ofValue("unit-kestra"))
            .containerImage(Property.ofValue("ghcr.io/kestra-io/dbt-bigquery:latest"))
            .commands(Property.ofValue(List.of("dbt build --project-dir unit-kestra")))
            .loadManifest(
                DbtCLI.KvStoreManifest.builder()
                    .key(Property.ofValue(MANIFEST_KEY))
                    .namespace(new Property<>(FLOW_NAMESPACE))
                    .build()
            )
            .build();

        RunContext runContextLoad = TestsUtils.mockRunContext(runContextFactory, loadManifest, Map.of());

        Path workingDir = runContextLoad.workingDir().path(true);
        copyFolder(Path.of(Objects.requireNonNull(this.getClass().getClassLoader().getResource("project")).getPath()),
            Path.of(runContextLoad.workingDir().path().toString(), "unit-kestra"));
        createSaFile(workingDir);
        String manifestValue = Files.readString(Path.of(
                Objects.requireNonNull(this.getClass().getClassLoader().getResource("manifest/manifest.json")).getPath())
            , StandardCharsets.UTF_8);
        var namespace = runContextLoad.render(FLOW_NAMESPACE);
        runContextLoad.namespaceKv(namespace).put(MANIFEST_KEY, new KVValueAndMetadata(null, manifestValue));

        ScriptOutput runOutputLoad = loadManifest.run(runContextLoad);

        assertThat(runOutputLoad.getExitCode(), is(0));
    }

    @Test
    void run_withLoadManifestString_shouldWriteJsonObjectManifest() throws Exception {
        var task = DbtCLI.builder()
            .id(IdUtils.create())
            .type(DbtCLI.class.getName())
            .taskRunner(Process.instance())
            .projectDir(Property.ofValue("unit-kestra"))
            .commands(Property.ofValue(List.of(
                "head -c 1 unit-kestra/target/manifest.json | grep -qx '{'"
            )))
            .loadManifest(
                DbtCLI.KvStoreManifest.builder()
                    .key(Property.ofValue(MANIFEST_KEY))
                    .namespace(new Property<>(FLOW_NAMESPACE))
                    .build()
            )
            .build();

        var runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());
        var manifest = Files.readString(
            Path.of(Objects.requireNonNull(this.getClass().getClassLoader().getResource("manifest/manifest.json")).getPath()),
            StandardCharsets.UTF_8
        );
        var namespace = runContext.render(FLOW_NAMESPACE);
        runContext.namespaceKv(namespace).put(MANIFEST_KEY, new KVValueAndMetadata(null, manifest));

        var runOutput = task.run(runContext);

        assertThat(runOutput.getExitCode(), is(0));
    }

    @Test
    void run_withWarning_shouldReturnWarningState() throws Exception {
        DbtCLI execute = DbtCLI.builder()
            .id(IdUtils.create())
            .type(DbtCLI.class.getName())
            .profiles(Property.ofValue(PROFILES))
            .containerImage(Property.ofValue("ghcr.io/kestra-io/dbt-bigquery:latest"))
            .commands(Property.ofValue(List.of(
                "dbt deps",
                "dbt run-operation emit_warning_log"
            )))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, execute, Map.of());

        Path workingDir = runContext.workingDir().path(true);
        copyFolder(Path.of(Objects.requireNonNull(this.getClass().getClassLoader().getResource("project")).getPath()), workingDir);
        createSaFile(workingDir);

        DbtCLI.Output runOutput = execute.run(runContext);

        assertThat("dbt command should succeed with exit code 0", runOutput.getExitCode(), is(0));
        assertThat("warningDetected flag should be true", runOutput.isWarningDetected(), is(true));
        assertThat("finalState should be present", runOutput.finalState().isPresent(), is(true));
        assertThat("finalState should be WARNING", runOutput.finalState().get(), is(State.Type.WARNING));
    }

    private void createSaFile(Path workingDir) throws IOException {
        String credentialsPath = System.getenv("GOOGLE_APPLICATION_CREDENTIALS");
        String encodedServiceAccount = System.getenv("GOOGLE_SERVICE_ACCOUNT");
        Assumptions.assumeTrue(
            (credentialsPath != null && !credentialsPath.isBlank()) ||
                (encodedServiceAccount != null && !encodedServiceAccount.isBlank()),
            "GOOGLE_APPLICATION_CREDENTIALS or GOOGLE_SERVICE_ACCOUNT must be set"
        );

        Path workingDirSa = workingDir.resolve("sa.json");
        if (credentialsPath != null && !credentialsPath.isBlank()) {
            Files.copy(Path.of(credentialsPath), workingDirSa);
            return;
        }

        try (var inputStream = new ByteArrayInputStream(Base64.getDecoder().decode(encodedServiceAccount.getBytes()))) {
            Files.copy(inputStream, workingDirSa);
        }
    }

    @Test
    void run_withFailure_shouldCaptureRunResults() throws Exception {
        DbtCLI execute = DbtCLI.builder()
            .id(IdUtils.create())
            .type(DbtCLI.class.getName())
            .profiles(Property.ofValue(PROFILES))
            .containerImage(Property.ofValue("ghcr.io/kestra-io/dbt-bigquery:latest"))
            .commands(Property.ofValue(List.of(
                "dbt deps",
                "mkdir -p models",
                "echo 'SELECT * FROM definitely_non_existent_table_12345' > models/failing_test_model.sql",
                "dbt run --models failing_test_model"
            )))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, execute, Map.of());

        Path workingDir = runContext.workingDir().path(true);
        copyFolder(Path.of(Objects.requireNonNull(this.getClass().getClassLoader().getResource("project")).getPath()), workingDir);
        createSaFile(workingDir);

        RunnableTaskException exception = assertThrows(RunnableTaskException.class, () -> {
            execute.run(runContext);
        });

        DbtCLI.Output output = (DbtCLI.Output) exception.getOutput();

        assertThat(output.getExitCode(), is(not(0)));
        assertThat(output.getOutputFiles(), is(notNullValue()));
        assertThat("run_results.json should be captured on failure",
            output.getOutputFiles(), hasKey("run_results.json"));
    }

    @Test
    void run_withFusionEngine_shouldUseFusionImage() throws Exception {
        DbtCLI execute = DbtCLI.builder()
            .id(IdUtils.create())
            .type(DbtCLI.class.getName())
            .profiles(Property.ofValue(PROFILES))
            .engine(Property.ofValue(DbtCLI.Engine.FUSION))
            .commands(Property.ofValue(List.of("dbt --version")))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, execute, Map.of());
        ScriptOutput runOutput = execute.run(runContext);

        assertThat(runOutput.getExitCode(), is(0));
    }

    @Test
    void run_withProjectDir_shouldInjectProjectDirFlag() throws Exception {
        DbtCLI task = DbtCLI.builder()
            .id(IdUtils.create())
            .type(DbtCLI.class.getName())
            .projectDir(Property.ofValue("unit-kestra"))
            .commands(Property.ofValue(List.of("dbt build")))
            .containerImage(Property.ofValue("ghcr.io/kestra-io/dbt-bigquery:latest"))
            .profiles(Property.ofValue(PROFILES))
            .build();

        var runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());

        Path workingDir = runContext.workingDir().path(true);
        Path projectDir = workingDir.resolve("unit-kestra");
        Files.createDirectories(projectDir);

        copyFolder(Path.of(Objects.requireNonNull(this.getClass().getClassLoader().getResource("project")).getPath()), projectDir);

        createSaFile(workingDir);

        var runOutput = task.run(runContext);

        assertThat(runOutput.getExitCode(), is(0));
    }
}
