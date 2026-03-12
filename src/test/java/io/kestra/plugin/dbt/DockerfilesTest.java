package io.kestra.plugin.dbt;

import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertTrue;

class DockerfilesTest {
    @Test
    void dbtFusionDockerfileDefinesShellBeforeInstallerRun() throws Exception {
        var dockerfile = Files.readString(Path.of("dockerfiles", "dbt-fusion.Dockerfile"));
        var shellIndex = dockerfile.indexOf("ENV SHELL=/bin/bash");
        var installerIndex = dockerfile.indexOf("install/install.sh");

        assertTrue(shellIndex >= 0, "dbt-fusion.Dockerfile must define SHELL for non-interactive builds");
        assertTrue(installerIndex > shellIndex, "SHELL must be defined before invoking the installer script");
    }
}
