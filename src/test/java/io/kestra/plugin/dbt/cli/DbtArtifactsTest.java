package io.kestra.plugin.dbt.cli;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@KestraTest
class DbtArtifactsTest {
    private static final String MANIFEST = "dbt/target/manifest.json";

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void restoreIfCaptured_shouldRestoreACapturedArtifactInANestedProjectDir() throws Exception {
        var runContext = runContext();
        var artifact = runContext.workingDir().path(true).resolve(MANIFEST);
        var captured = capture(runContext, artifact, "captured");

        DbtArtifacts.restoreIfCaptured(runContext, artifact.toFile(), Map.of(MANIFEST, captured));

        assertThat(Files.readString(artifact), is("captured"));
    }

    @Test
    void restoreIfCaptured_shouldKeepTheArtifactOnDisk() throws Exception {
        var runContext = runContext();
        var artifact = runContext.workingDir().path(true).resolve(MANIFEST);
        var captured = capture(runContext, artifact, "captured");
        Files.writeString(artifact, "on disk");

        DbtArtifacts.restoreIfCaptured(runContext, artifact.toFile(), Map.of(MANIFEST, captured));

        assertThat(Files.readString(artifact), is("on disk"));
    }

    @Test
    void restoreIfCaptured_shouldDoNothingWithoutOutputFiles() throws Exception {
        var runContext = runContext();
        var artifact = runContext.workingDir().path(true).resolve(MANIFEST);

        DbtArtifacts.restoreIfCaptured(runContext, artifact.toFile(), null);

        assertThat(Files.exists(artifact), is(false));
    }

    @Test
    void restoreIfCaptured_shouldDoNothingWhenTheArtifactWasNotCaptured() throws Exception {
        var runContext = runContext();
        var artifact = runContext.workingDir().path(true).resolve(MANIFEST);
        var other = capture(runContext, runContext.workingDir().path(true).resolve("dbt/target/catalog.json"), "{}");

        DbtArtifacts.restoreIfCaptured(runContext, artifact.toFile(), Map.of("dbt/target/catalog.json", other));

        assertThat(Files.exists(artifact), is(false));
    }

    @Test
    void restoreIfCaptured_shouldNotThrowWhenTheCapturedFileCannotBeRead() throws Exception {
        var runContext = runContext();
        var artifact = runContext.workingDir().path(true).resolve(MANIFEST);

        DbtArtifacts.restoreIfCaptured(runContext, artifact.toFile(), Map.of(MANIFEST, URI.create("kestra:///missing/manifest.json")));

        assertThat(Files.exists(artifact), is(false));
    }

    private RunContext runContext() {
        var task = DbtCLI.builder()
            .id(IdUtils.create())
            .type(DbtCLI.class.getName())
            .commands(Property.ofValue(List.of("dbt build")))
            .build();
        return TestsUtils.mockRunContext(runContextFactory, task, Map.of());
    }

    private URI capture(RunContext runContext, Path file, String content) throws IOException {
        Files.createDirectories(file.getParent());
        Files.writeString(file, content);
        return runContext.storage().putFile(file.toFile());
    }
}
