package io.kestra.plugin.dbt.cloud;

import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.micronaut.context.annotation.Value;
import io.kestra.core.junit.annotations.KestraTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

@KestraTest
class TriggerRunTest {
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
        TriggerRun task = TriggerRun.builder()
            .id(IdUtils.create())
            .type(TriggerRun.class.getName())
            .accountId(this.accountId)
            .token(this.token)
            .jobId(this.jobId)
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());

        TriggerRun.Output run = task.run(runContext);

        assertThat(run.getRunId(), is(notNullValue()));
        assertThat(runContext.dynamicWorkerResults().size(), is(13));
    }
}
