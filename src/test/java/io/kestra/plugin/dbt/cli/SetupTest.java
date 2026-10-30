package io.kestra.plugin.dbt.cli;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.core.runner.Process;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;

@KestraTest
class SetupTest {
    private static final String PAYLOAD = "{{ 1 + 1 }}";

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void shouldKeepPebbleExpressionWhenRenderedInputContainsOne() throws Exception {
        var setup = Setup.builder()
            .id(IdUtils.create())
            .type(Setup.class.getName())
            .taskRunner(Process.instance())
            .pythonPath("true")
            // fake pip that logs its arguments, so no network or python is needed
            .beforeCommands(
                Property.ofValue(
                    List.of(
                        "mkdir -p bin && printf '#!/bin/sh\\necho \"$@\" >> pip.log\\n' > bin/pip && chmod +x bin/pip"
                    )
                )
            )
            .requirements(Property.ofExpression("[\"{{ inputs.payload }}\"]"))
            .profiles(Property.ofValue(Map.of("target", PAYLOAD)))
            .build();

        var runContext = TestsUtils.mockRunContext(runContextFactory, setup, Map.of("payload", PAYLOAD));

        setup.run(runContext);

        var pipLog = Files.readString(runContext.workingDir().resolve(Path.of("pip.log")), StandardCharsets.UTF_8);
        var profiles = Files.readString(runContext.workingDir().resolve(Path.of(".profile/profiles.yml")), StandardCharsets.UTF_8);

        assertThat(pipLog, containsString("install " + PAYLOAD));
        assertThat(profiles, containsString(PAYLOAD));
    }
}
