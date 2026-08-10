package io.kestra.plugin.dbt.cloud;

import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;

import io.kestra.core.http.HttpRequest;
import io.kestra.core.http.HttpResponse;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.dbt.cloud.models.Run;
import io.kestra.plugin.dbt.cloud.models.RunListResponse;
import io.kestra.plugin.dbt.cloud.models.RunResponse;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Start a dbt Cloud job run",
    description = "Triggers a dbt Cloud job via API. Optionally waits for completion to stream logs, surface dynamic steps, and collect run results; wait defaults to true with 5s polling and a 60m cap."
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
                    accountId: "12345"
                    token: "{{ secret('DBT_TOKEN') }}"
                    jobId: "67890"
                """
        )
    }
)
public class TriggerRun extends AbstractDbtCloud implements RunnableTask<TriggerRun.Output> {

    @Schema(
        title = "Job ID",
        description = "Numeric dbt Cloud job identifier to trigger."
    )
    @NotNull
    Property<String> jobId;

    @Schema(
        title = "Run cause",
        description = "Reason passed to dbt Cloud. Defaults to \"Triggered by Kestra.\""
    )
    @Builder.Default
    @NotNull
    Property<String> cause = Property.ofValue("Triggered by Kestra.");

    @Schema(
        title = "Git SHA override",
        description = "Specific commit to checkout before the run."
    )
    Property<String> gitSha;

    @Schema(
        title = "Git branch override",
        description = "Branch to checkout when triggering the job."
    )
    Property<String> gitBranch;

    @Schema(
        title = "Schema override",
        description = "Destination schema to use instead of the job target default."
    )
    Property<String> schemaOverride;

    @Schema(
        title = "dbt version override",
        description = "dbt version string to force for this run."
    )
    Property<String> dbtVersionOverride;

    @Schema(
        title = "Threads override",
        description = "Thread count for the run."
    )
    Property<String> threadsOverride;

    @Schema(
        title = "Target name override",
        description = "Value for the `target.name` context variable."
    )
    Property<String> targetNameOverride;

    @Schema(
        title = "Generate docs override",
        description = "Whether the run builds docs even if the job is configured otherwise."
    )
    Property<Boolean> generateDocsOverride;

    @Schema(
        title = "Timeout override",
        description = "Job timeout in seconds for this run."
    )
    Property<Integer> timeoutSecondsOverride;

    @Schema(
        title = "Steps override",
        description = "Custom steps list executed instead of the job defaults."
    )
    Property<List<String>> stepsOverride;

    @Schema(
        title = "Wait for completion",
        description = "If true (default), polls dbt Cloud until the run ends and streams logs and artifacts."
    )
    @Builder.Default
    Property<Boolean> wait = Property.ofValue(Boolean.TRUE);

    @Schema(
        title = "Reattach to an in-flight run",
        description = "If true, on a worker restart or retry the task reattaches to the run it already started and " +
            "waits for it instead of triggering a duplicate. It only reattaches to a run this execution started " +
            "(matched by the taskrun id in the run cause), never one triggered elsewhere. Default false, which " +
            "always triggers a new run."
    )
    @Builder.Default
    @PluginProperty(group = "reliability")
    Property<Boolean> reattach = Property.ofValue(Boolean.FALSE);

    @Schema(
        title = "Poll frequency",
        description = "Interval between status checks when waiting. Default 5s."
    )
    @Builder.Default
    Property<Duration> pollFrequency = Property.ofValue(Duration.ofSeconds(5));

    @Schema(
        title = "Max wait duration",
        description = "Ceiling for waiting on job completion. Default 60m."
    )
    @Builder.Default
    Property<Duration> maxDuration = Property.ofValue(Duration.ofMinutes(60));

    @Builder.Default
    @Schema(
        title = "Parse run results",
        description = "If true (default), parses dbt run results to expose node durations and warnings."
    )
    @PluginProperty(group = "advanced")
    protected Property<Boolean> parseRunResults = Property.ofValue(Boolean.TRUE);

    @Override
    public TriggerRun.Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();

        if (Boolean.TRUE.equals(runContext.render(this.reattach).as(Boolean.class).orElse(Boolean.FALSE))) {
            Run inFlightRun = this.findInFlightRun(runContext);
            if (inFlightRun != null) {
                logger.info(
                    "Reattached to in-flight run {} (status {}) instead of triggering a new one",
                    inFlightRun.getId(), inFlightRun.getStatus()
                );
                return this.waitOrReturn(runContext, inFlightRun.getId());
            }
        }

        // trigger
        Map<String, Object> body = new HashMap<>();
        // Tag the cause with the taskrun id so a later reattach can find the exact run this execution started.
        body.put("cause", runContext.render(this.cause).as(String.class).orElseThrow() + " " + taskrunTag(runContext));

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
            .uri(
                URI.create(
                    runContext.render(this.baseUrl).as(String.class).orElseThrow() + "/api/v2/accounts/" + runContext.render(this.accountId).as(String.class).orElseThrow() +
                        "/jobs/" + runContext.render(this.jobId).as(String.class).orElseThrow() + "/run/"
                )
            )
            .method("POST")
            .body(
                HttpRequest.JsonRequestBody.builder()
                    .content(body)
                    .build()
            );

        HttpResponse<RunResponse> triggerResponse = this.request(runContext, requestBuilder, RunResponse.class);

        RunResponse triggerRunResponse = triggerResponse.getBody();
        if (triggerRunResponse == null) {
            throw new IllegalStateException("Missing body on trigger");
        }

        logger.info("Job status {} with response: {}", triggerResponse.getStatus(), triggerRunResponse);

        return this.waitOrReturn(runContext, triggerRunResponse.getData().getId());
    }

    /**
     * Look for an in-flight run of the job (Queued, Starting, or Running) that this taskrun started,
     * matched by the taskrun tag in the run cause. Returns null when there is none, in which case a
     * fresh run is triggered rather than attaching to a run this execution did not start.
     */
    private Run findInFlightRun(RunContext runContext) throws Exception {
        HttpRequest.HttpRequestBuilder requestBuilder = HttpRequest.builder()
            .uri(
                URI.create(
                    runContext.render(this.baseUrl).as(String.class).orElseThrow() + "/api/v2/accounts/" + runContext.render(this.accountId).as(String.class).orElseThrow() +
                        "/runs/?job_definition_id=" + runContext.render(this.jobId).as(String.class).orElseThrow() +
                        "&status__in=" + URLEncoder.encode("[1,2,3]", StandardCharsets.UTF_8) +
                        "&order_by=-id" +
                        "&include_related=" + URLEncoder.encode("[\"trigger\"]", StandardCharsets.UTF_8)
                )
            )
            .method("GET");

        HttpResponse<RunListResponse> response = this.request(runContext, requestBuilder, RunListResponse.class);

        RunListResponse listResponse = response.getBody();
        if (listResponse == null || listResponse.getData() == null || listResponse.getData().isEmpty()) {
            return null;
        }

        String tag = taskrunTag(runContext);
        return listResponse.getData().stream()
            .filter(
                run -> run.getTrigger() != null
                    && run.getTrigger().getCause() != null
                    && run.getTrigger().getCause().contains(tag)
            )
            .findFirst()
            .orElse(null);
    }

    /** The marker embedded in a run's cause so a reattach can identify the run this taskrun started. */
    private static String taskrunTag(RunContext runContext) {
        return "[taskrun:" + runContext.taskRunInfo().taskRunId() + "]";
    }

    private Output waitOrReturn(RunContext runContext, Long runId) throws Exception {
        if (Boolean.FALSE.equals(runContext.render(this.wait).as(Boolean.class).orElse(Boolean.TRUE))) {
            return Output.builder()
                .runId(runId)
                .build();
        }

        CheckStatus checkStatusJob = CheckStatus.builder()
            .runId(Property.ofValue(runId.toString()))
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
            title = "Run ID",
            description = "dbt Cloud run identifier returned by the trigger call."
        )
        private Long runId;

        @Schema(
            title = "Run results URI",
            description = "Internal storage URI for `run_results.json`, when available."
        )
        private URI runResults;

        @Schema(
            title = "Manifest URI",
            description = "Internal storage URI for `manifest.json`, when available."
        )
        private URI manifest;
    }
}
