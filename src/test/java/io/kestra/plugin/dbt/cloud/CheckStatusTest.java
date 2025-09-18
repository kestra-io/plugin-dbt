package io.kestra.plugin.dbt.cloud;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.micronaut.context.annotation.Value;
import io.kestra.core.junit.annotations.KestraTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

@KestraTest
class CheckStatusTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Value("${dbt.cloud.account-id}")
    private String accountId;

    @Value("${dbt.cloud.token}")
    private String token;

    @Value("${dbt.cloud.job-id}")
    private String jobId;

    @Test
    @Disabled("Trial account can't trigger run through api")
    void run() throws Exception {

        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        TriggerRun task = TriggerRun.builder()
                .id(IdUtils.create())
                .type(TriggerRun.class.getName())
                .accountId(Property.ofValue(this.accountId))
                .wait(Property.ofValue(false))
                .token(Property.ofValue(this.token))
                .jobId(Property.ofValue(this.jobId))
                .build();

        TriggerRun.Output runOutput = task.run(runContext);

        CheckStatus checkStatus = CheckStatus.builder()
                .runId(Property.ofValue(runOutput.getRunId().toString()))
                .token(Property.ofValue(this.token))
                .accountId(Property.ofValue(this.accountId))
                .maxDuration(Property.ofValue(Duration.ofMinutes(60)))
                .parseRunResults(Property.ofValue(false))
                .build();

        CheckStatus.Output checkStatusOutput = checkStatus.run(runContext);

        assertThat(checkStatusOutput, is(notNullValue()));
        assertThat(checkStatusOutput.getManifest(), is(notNullValue()));
    }
}
