package io.kestra.plugin.dbt.cli;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import io.kestra.core.runners.RunContext;

/**
 * An {@code outputFiles} glob such as {@code **}{@code /*.json} also matches dbt's {@code target/manifest.json} and
 * {@code target/run_results.json}. Capturing an output file uploads it to internal storage and deletes the local copy,
 * so by the time the task parses the dbt artifacts they are no longer on disk. This puts them back.
 */
final class DbtArtifacts {
    private DbtArtifacts() {
    }

    static void restoreIfCaptured(RunContext runContext, File artifact, Map<String, URI> outputFiles) throws IOException {
        if (artifact.exists() || outputFiles == null) {
            return;
        }

        // output files are keyed by their path relative to the working directory, see FilesService.outputFiles
        Path artifactPath = artifact.toPath().normalize();
        URI captured = outputFiles.get(runContext.workingDir().path().relativize(artifactPath).toString());
        if (captured == null) {
            return;
        }

        Files.createDirectories(artifactPath.getParent());
        try (InputStream is = runContext.storage().getFile(captured)) {
            Files.copy(is, artifactPath);
        }
    }
}
