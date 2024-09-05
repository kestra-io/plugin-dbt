package io.kestra.plugin.dbt.cloud;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.dbt.cloud.models.RunResponse;
import io.micronaut.core.type.Argument;
import io.micronaut.http.HttpMethod;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.uri.UriTemplate;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;

import java.net.URI;
import java.time.Duration;
import java.util.*;
import jakarta.validation.constraints.NotNull;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Trigger job to run.",
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
    @PluginProperty(dynamic = true)
    @NotNull
    String jobId;

    @Schema(
        title = "A text description of the reason for running this job."
    )
    @PluginProperty(dynamic = true)
    @Builder.Default
    @NotNull
    String cause = "Triggered by Kestra.";

    @Schema(
        title = "The git SHA to check out before running this job."
    )
    @PluginProperty(dynamic = true)
    String gitSha;

    @Schema(
        title = "The git branch to check out before running this job."
    )
    @PluginProperty(dynamic = true)
    String gitBranch;

    @Schema(
        title = "Override the destination schema in the configured target for this job."
    )
    @PluginProperty(dynamic = true)
    String schemaOverride;

    @Schema(
        title = "Override the version of dbt used to run this job."
    )
    @PluginProperty(dynamic = true)
    String dbtVersionOverride;

    @Schema(
        title = "Override the number of threads used to run this job."
    )
    @PluginProperty(dynamic = true)
    String threadsOverride;

    @Schema(
        title = "Override the target.name context variable used when running this job."
    )
    @PluginProperty(dynamic = true)
    String targetNameOverride;

    @Schema(
        title = "Override whether or not this job generates docs."
    )
    @PluginProperty(dynamic = false)
    Boolean generateDocsOverride;

    @Schema(
        title = "Override the timeout in seconds for this job."
    )
    @PluginProperty(dynamic = false)
    Integer timeoutSecondsOverride;

    @Schema(
        title = "Override the list of steps for this job."
    )
    @PluginProperty(dynamic = true)
    List<String> stepsOverride;

    @Schema(
        title = "Wait for the end of the run.",
        description = "Allowing to capture job status & logs."
    )
    @PluginProperty(dynamic = false)
    @Builder.Default
    Boolean wait = true;

    @Schema(
            title = "Specify frequency for job state check API calls."
    )
    @PluginProperty(dynamic = false)
    @Builder.Default
    Duration pollFrequency = Duration.ofSeconds(5);

    @Schema(
        title = "The maximum total wait duration."
    )
    @PluginProperty(dynamic = false)
    @Builder.Default
    Duration maxDuration = Duration.ofMinutes(60);

    @Builder.Default
    @Schema(
        title = "Parse run result.",
        description = "Parsing run result to display duration of each task inside dbt."
    )
    @PluginProperty
    protected Boolean parseRunResults = true;

    @Override
    public TriggerRun.Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();

        // trigger
        Map<String, Object> body = new HashMap<>();
        body.put("cause", runContext.render(this.cause));

        if (this.gitSha != null) {
            body.put("git_sha", runContext.render(this.gitSha));
        }

        if (this.gitBranch != null) {
            body.put("git_branch", runContext.render(this.gitBranch));
        }

        if (this.schemaOverride != null) {
            body.put("schema_override", runContext.render(this.schemaOverride));
        }

        if (this.dbtVersionOverride != null) {
            body.put("dbt_version_override", runContext.render(this.dbtVersionOverride));
        }

        if (this.threadsOverride != null) {
            body.put("threads_override", runContext.render(this.threadsOverride));
        }

        if (this.targetNameOverride != null) {
            body.put("target_name_override", runContext.render(this.targetNameOverride));
        }

        if (this.targetNameOverride != null) {
            body.put("target_name_override", runContext.render(this.targetNameOverride));
        }

        if (this.generateDocsOverride != null) {
            body.put("generate_docs_override", this.generateDocsOverride);
        }

        if (this.timeoutSecondsOverride != null) {
            body.put("timeout_seconds_override", this.timeoutSecondsOverride);
        }

        if (this.stepsOverride != null) {
            body.put("steps_override", runContext.render(this.stepsOverride));
        }

        HttpResponse<RunResponse> triggerResponse = this.request(
            runContext,
            HttpRequest
                .create(
                    HttpMethod.POST,
                    UriTemplate
                        .of("/api/v2/accounts/{accountId}/jobs/{jobId}/run")
                        .expand(Map.of(
                            "accountId", runContext.render(this.accountId),
                            "jobId", runContext.render(this.jobId)
                        )) + "/"
                )
                .body(body),
            Argument.of(RunResponse.class)
        );

        RunResponse triggerRunResponse = triggerResponse.getBody().orElseThrow(() -> new IllegalStateException("Missing body on trigger"));
        logger.info("Job status {} with response: {}", triggerResponse.getStatus(), triggerRunResponse);
        Long runId = triggerRunResponse.getData().getId();

        if (!this.wait) {
            return Output.builder()
                .runId(runId)
                .build();
        }

        CheckStatus checkStatusJob = CheckStatus.builder()
                                        .runId(runId.toString())
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
