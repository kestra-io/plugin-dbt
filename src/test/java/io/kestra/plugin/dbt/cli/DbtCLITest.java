package io.kestra.plugin.dbt.cli;

import io.kestra.core.models.flows.State;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTaskException;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.storages.kv.KVStore;
import io.kestra.core.storages.kv.KVValueAndMetadata;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.scripts.exec.scripts.models.ScriptOutput;
import io.kestra.core.junit.annotations.KestraTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
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
    StorageInterface storageInterface;

    @Inject
    private RunContextFactory runContextFactory;

    private static final String NAMESPACE_ID = "io.kestra.plugin.dbt.cli.dbtclitest";

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
            .profiles(Property.of(PROFILES)
            )
            .logFormat(Property.of(logFormat))
            .containerImage(new Property<>("ghcr.io/kestra-io/dbt-bigquery:latest"))
            .commands(Property.of(List.of("dbt build")))
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
            .profiles(Property.of(PROFILES)
            )
            .containerImage(new Property<>("ghcr.io/kestra-io/dbt-bigquery:latest"))
            .commands(Property.of(List.of("dbt build")))
            .storeManifest(
                DbtCLI.KvStoreManifest.builder()
                    .key(Property.of(MANIFEST_KEY))
                    .namespace(Property.of(NAMESPACE_ID))
                    .build()
            )
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, execute, Map.of());

        Path workingDir = runContext.workingDir().path(true);
        copyFolder(Path.of(Objects.requireNonNull(this.getClass().getClassLoader().getResource("project")).getPath()), workingDir);
        createSaFile(workingDir);

        ScriptOutput runOutput = execute.run(runContext);

        assertThat(runOutput.getExitCode(), is(0));
        KVStore kvStore = runContext.namespaceKv(NAMESPACE_ID);
        assertThat(kvStore.get(MANIFEST_KEY).isPresent(), is(true));
        Map<String, Object> manifestValue = (Map<String, Object>) kvStore.getValue(MANIFEST_KEY).get().value();
        assertThat(((Map<String, Object>) manifestValue.get("metadata")).get("project_name"), is("unit_kestra"));
    }

    @Disabled("To run put a manifest.json under src/test/resources/manifest/")
    @Test
    void testDbtWithLoadManifest_manifestShouldBeLoadedFromKvStore() throws Exception {
        DbtCLI loadManifest = DbtCLI.builder()
            .id(IdUtils.create())
            .type(DbtCLI.class.getName())
            .profiles(Property.of(PROFILES))
            .projectDir(Property.of("unit-kestra"))
            .containerImage(new Property<>("ghcr.io/kestra-io/dbt-bigquery:latest"))
            .commands(Property.of(List.of("dbt build --project-dir unit-kestra")))
            .loadManifest(
                DbtCLI.KvStoreManifest.builder()
                    .key(Property.of(MANIFEST_KEY))
                    .namespace(Property.of(NAMESPACE_ID))
                    .build()
            )
            .build();

        RunContext runContextLoad = TestsUtils.mockRunContext(runContextFactory, loadManifest, Map.of());

        Path workingDir = runContextLoad.workingDir().path(true);
        copyFolder(Path.of(Objects.requireNonNull(this.getClass().getClassLoader().getResource("project")).getPath()),
            Path.of(runContextLoad.workingDir().path().toString(),"unit-kestra"));
        createSaFile(workingDir);
        String manifestValue = Files.readString(Path.of(
            Objects.requireNonNull(this.getClass().getClassLoader().getResource("manifest/manifest.json")).getPath())
            , StandardCharsets.UTF_8);
        runContextLoad.namespaceKv(NAMESPACE_ID).put(MANIFEST_KEY, new KVValueAndMetadata(null, manifestValue));

        ScriptOutput runOutputLoad = loadManifest.run(runContextLoad);

        assertThat(runOutputLoad.getExitCode(), is(0));
    }

    @Test
    void run_withWarning_shouldReturnWarningState() throws Exception {
        DbtCLI execute = DbtCLI.builder()
            .id(IdUtils.create())
            .type(DbtCLI.class.getName())
            .profiles(Property.of(PROFILES))
            .containerImage(new Property<>("ghcr.io/kestra-io/dbt-bigquery:latest"))
            .commands(Property.of(List.of(
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
        Path existingSa = Path.of(System.getenv("GOOGLE_APPLICATION_CREDENTIALS"));
        Path workingDirSa = workingDir.resolve("sa.json");
        Files.copy(existingSa, workingDirSa);
    }

    @Test
    void run_withFailure_shouldCaptureRunResults() throws Exception {
        DbtCLI execute = DbtCLI.builder()
            .id(IdUtils.create())
            .type(DbtCLI.class.getName())
            .profiles(Property.ofValue(PROFILES))
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
}
