package io.kestra.plugin.dbt.cli;

import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.storages.kv.KVStore;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.scripts.exec.scripts.models.ScriptOutput;
import io.kestra.core.junit.annotations.KestraTest;
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

@KestraTest
class DbtCLITest {
    @Inject
    StorageInterface storageInterface;

    @Inject
    private RunContextFactory runContextFactory;

    private final String NAMESPACE_ID = "io.kestra.plugin.dbt.cli.dbtclitest";

    private final String MANIFEST_KEY = "manifest.json";

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

    @Test
    void run() throws Exception {
        DbtCLI execute = DbtCLI.builder()
            .id(IdUtils.create())
            .type(DbtCLI.class.getName())
            .profiles(Property.of(PROFILES)
            )
            .containerImage("ghcr.io/kestra-io/dbt-bigquery:latest")
            .commands(List.of("dbt build"))
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
            .containerImage("ghcr.io/kestra-io/dbt-bigquery:latest")
            .commands(List.of("dbt build"))
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

    private void createSaFile(Path workingDir) throws IOException {
        Path existingSa = Path.of(System.getenv("GOOGLE_APPLICATION_CREDENTIALS"));
        Path workingDirSa = workingDir.resolve("sa.json");
        Files.copy(existingSa, workingDirSa);
    }
}
