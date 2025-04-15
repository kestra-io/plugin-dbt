package io.kestra.plugin.dbt.cloud;

import io.kestra.core.http.HttpRequest;
import io.kestra.core.http.HttpResponse;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.dbt.cloud.models.RunResponse;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;

import java.net.URI;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Trigger a dbt cloud job to run.",
    description = "Use this task to kick off a run for a job. When this endpoint returns a successful response, a " +
        "new run will be enqueued for the account. If you activate the `wait` option, it will wait for the job to be ended " +
        "and will display all the log and dynamic tasks."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            code = """
                id: dbt_trigger_job_run
                namespace: company.team

                tasks:
                  - id: trigger_run
                    type: io.kestra.plugin.dbt.cloud.TriggerRun
                    accountId: "dbt_account"
                    token: "dbt_token"
                    jobId: "job_id"
                """
        )
    }
)
public class TriggerRun extends AbstractDbtCloud implements RunnableTask<TriggerRun.Output> {

    @Schema(
        title = "Numeric ID of the job."
    )
    @NotNull
    Property<String> jobId;

    @Schema(
        title = "A text description of the reason for running this job."
    )
    @Builder.Default
    @NotNull
    Property<String> cause = Property.of("Triggered by Kestra.");

    @Schema(
        title = "The git SHA to check out before running this job."
    )
    Property<String> gitSha;

    @Schema(
        title = "The git branch to check out before running this job."
    )
    Property<String> gitBranch;

    @Schema(
        title = "Override the destination schema in the configured target for this job."
    )
    Property<String> schemaOverride;

    @Schema(
        title = "Override the version of dbt used to run this job."
    )
    Property<String> dbtVersionOverride;

    @Schema(
        title = "Override the number of threads used to run this job."
    )
    Property<String> threadsOverride;

    @Schema(
        title = "Override the target.name context variable used when running this job."
    )
    Property<String> targetNameOverride;

    @Schema(
        title = "Override whether or not this job generates docs."
    )
    Property<Boolean> generateDocsOverride;

    @Schema(
        title = "Override the timeout in seconds for this job."
    )
    Property<Integer> timeoutSecondsOverride;

    @Schema(
        title = "Override the list of steps for this job."
    )
    Property<List<String>> stepsOverride;

    @Schema(
        title = "Wait for the end of the run.",
        description = "Allowing to capture job status & logs."
    )
    @Builder.Default
    Property<Boolean> wait = Property.of(Boolean.TRUE);

    @Schema(
            title = "Specify frequency for job state check API calls."
    )
    @Builder.Default
    Property<Duration> pollFrequency = Property.of(Duration.ofSeconds(5));

    @Schema(
        title = "The maximum total wait duration."
    )
    @Builder.Default
    Property<Duration> maxDuration = Property.of(Duration.ofMinutes(60));

    @Builder.Default
    @Schema(
        title = "Parse run result.",
        description = "Parsing run result to display duration of each task inside dbt."
    )
    protected Property<Boolean> parseRunResults = Property.of(Boolean.TRUE);

    @Override
    public TriggerRun.Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();

        // trigger
        Map<String, Object> body = new HashMap<>();
        body.put("cause", runContext.render(this.cause).as(String.class).orElseThrow());

        runContext.render(this.gitSha).as(String.class).ifPresent(sha -> body.put("git_sha", sha));
        runContext.render(this.gitBranch).as(String.class).ifPresent(branch -> body.put("git_branch", branch));
        runContext.render(this.schemaOverride).as(String.class).ifPresent(schema -> body.put("schema_override", schema));
        runContext.render(this.dbtVersionOverride).as(String.class).ifPresent(version -> body.put("dbt_version_override", version));
        runContext.render(this.threadsOverride).as(String.class).ifPresent(thread -> body.put("threads_override", thread));
        runContext.render(this.targetNameOverride).as(String.class).ifPresent(target -> body.put("target_name_override", target));
        runContext.render(this.generateDocsOverride).as(Boolean.class).ifPresent(doc -> body.put("generate_docs_override", doc));
        runContext.render(this.timeoutSecondsOverride).as(Integer.class).ifPresent(timeout -> body.put("timeout_seconds_override", timeout));

        if (!runContext.render(this.stepsOverride).asList(String.class).isEmpty()) {
            body.put("steps_override", runContext.render(this.stepsOverride).asList(String.class));
        }

        HttpRequest.HttpRequestBuilder requestBuilder = HttpRequest.builder()
            .uri(URI.create(runContext.render(this.baseUrl).as(String.class).orElseThrow() + "/api/v2/accounts/" + runContext.render(this.accountId).as(String.class).orElseThrow() +
                "/jobs/" + runContext.render(this.jobId).as(String.class).orElseThrow() + "/run/"))
            .method("POST")
            .body(HttpRequest.JsonRequestBody.builder()
                .content(body)
                .build());

        HttpResponse<RunResponse> triggerResponse = this.request(runContext, requestBuilder, RunResponse.class);

        RunResponse triggerRunResponse = triggerResponse.getBody();
        if (triggerRunResponse == null) {
            throw new IllegalStateException("Missing body on trigger");
        }

        logger.info("Job status {} with response: {}", triggerResponse.getStatus(), triggerRunResponse);
        Long runId = triggerRunResponse.getData().getId();

        if (Boolean.FALSE.equals(runContext.render(this.wait).as(Boolean.class).orElse(Boolean.TRUE))) {
            return Output.builder()
                .runId(runId)
                .build();
        }

        CheckStatus checkStatusJob = CheckStatus.builder()
            .runId(Property.of(runId.toString()))
            .baseUrl(getBaseUrl())
            .token(getToken())
            .accountId(getAccountId())
            .pollFrequency(getPollFrequency())
            .maxDuration(getMaxDuration())
            .parseRunResults(getParseRunResults())
            .build();

        CheckStatus.Output runOutput = checkStatusJob.run(runContext);

        return Output.builder()
            .runId(runId)
            .runResults(runOutput.getRunResults())
            .manifest(runOutput.getManifest())
            .build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "The run ID."
        )
        private Long runId;

        @Schema(
            title = "URI of a run result."
        )
        private URI runResults;

        @Schema(
            title = "URI of a manifest."
        )
        private URI manifest;
    }
}
