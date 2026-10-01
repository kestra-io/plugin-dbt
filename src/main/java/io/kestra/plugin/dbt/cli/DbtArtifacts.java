package io.kestra.plugin.dbt.cli;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Map;

import io.kestra.core.runners.RunContext;

final class DbtArtifacts {
    private DbtArtifacts() {
    }

    static void restoreIfCaptured(RunContext runContext, File artifact, Map<String, URI> outputFiles) {
        if (artifact.exists() || outputFiles == null) {
            return;
        }

        var artifactPath = artifact.toPath().normalize();
        var captured = outputFiles.get(runContext.workingDir().path().relativize(artifactPath).toString());
        if (captured == null) {
            return;
        }

        // capturing an output file deletes the local copy, so re-fetch it from storage
        try (var is = runContext.storage().getFile(captured)) {
            Files.createDirectories(artifactPath.getParent());
            Files.copy(is, artifactPath, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            runContext.logger().warn("Unable to restore the captured dbt artifact '{}', it will not be parsed", artifact.getName(), e);
        }
    }
}
