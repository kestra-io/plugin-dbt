package io.kestra.plugin.dbt.cloud;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.micronaut.context.annotation.Value;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

@MicronautTest
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
    // @Disabled("Trial account can't trigger run through api")
    void run() throws Exception {

        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        TriggerRun task = TriggerRun.builder()
                .id(IdUtils.create())
                .type(TriggerRun.class.getName())
                .accountId(this.accountId)
                .wait(false)
                .token(this.token)
                .jobId(this.jobId)
                .build();

        TriggerRun.Output runOutput = task.run(runContext);


        CheckStatus checkStatus = CheckStatus.builder()
                .runId(runOutput.getRunId())
                .maxDuration(Duration.ofMinutes(60))
                .build();

        CheckStatus.Output checkStatusOutput = checkStatus.run(runContext);

        assertThat(checkStatusOutput, is(notNullValue()));
        assertThat(checkStatusOutput.getManifest(), is(notNullValue()));
    }
}
