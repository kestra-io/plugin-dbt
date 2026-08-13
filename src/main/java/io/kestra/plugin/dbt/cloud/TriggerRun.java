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
import io.kestra.plugin.dbt.cloud.models.JobStatus;
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

    // Bounded retry for the post-failure confirm-and-adopt lookup: a run that was just created by an
    // ambiguous (possibly-sent) trigger call can take a moment to show up in the run list.
    private static final int CONFIRM_LOOKUP_MAX_ATTEMPTS = 3;
    private static final Duration CONFIRM_LOOKUP_BACKOFF = Duration.ofSeconds(1);

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
        description = """
            If true, the task reattaches to a run it already started instead of triggering a duplicate. This is \
            checked at the start of a worker restart or retry: a run this taskrun started that is still queued, \
            starting, running, or already succeeded is adopted, so a delayed retry after a lost response does not \
            trigger a duplicate. A run that genuinely failed or was cancelled is NOT adopted at this point, so a \
            task-level `retry` or a manual "Restart from failed task" still triggers a fresh run instead of \
            silently re-reporting the old failure. It is also checked when the trigger call itself fails with an \
            ambiguous error (e.g. a timeout) that may mean dbt Cloud already received it: in that case any \
            matching run is adopted regardless of status, since it was just created by the call that timed out and \
            its real outcome (success or failure) must be reported for this attempt. It only reattaches to a run \
            this execution started (matched by the taskrun id in the run cause), never one triggered elsewhere. \
            Default false, which always triggers a new run."""
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

        boolean reattachEnabled = Boolean.TRUE.equals(runContext.render(this.reattach).as(Boolean.class).orElse(Boolean.FALSE));

        if (reattachEnabled) {
            var existingRun = this.findReattachableRunForThisTaskRun(runContext);
            if (existingRun != null) {
                logger.info(
                    "Reattached to run {} (status {}) already started by this taskrun instead of triggering a new one",
                    existingRun.getId(), existingRun.getStatus()
                );
                return this.waitOrReturn(runContext, existingRun.getId());
            }
        }

        // trigger
        Map<String, Object> body = new HashMap<>();
        String cause = runContext.render(this.cause).as(String.class).orElseThrow();
        if (reattachEnabled) {
            // Only when reattach is on: tag the cause with the taskrun id so a later attempt finds this exact run.
            // When reattach is off the cause is left untouched, so upgrading changes nothing for existing flows.
            cause = cause + " " + taskrunTag(runContext);
        }
        body.put("cause", cause);

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

        HttpResponse<RunResponse> triggerResponse;
        try {
            triggerResponse = this.request(runContext, requestBuilder, RunResponse.class);
        } catch (Exception e) {
            // Only when reattach is on (a marker was written above) is a lookup reliable: an ambiguous
            // failure (read timeout, mid-flight drop) may mean dbt Cloud already created the run, so
            // confirm before failing loud and creating a duplicate on the next retry.
            if (reattachEnabled && wasPossiblySent(e)) {
                Run adoptedRun = null;
                try {
                    adoptedRun = this.confirmRunAfterAmbiguousFailure(runContext);
                } catch (Exception confirmEx) {
                    // The confirm-lookup itself failing (the GET exhausting retries, an interrupted
                    // backoff) must never mask the original trigger failure below: log and fall
                    // through so `e` is the one rethrown.
                    logger.warn("Could not confirm whether the ambiguous trigger call created a run: {}", confirmEx.getMessage());
                }
                if (adoptedRun != null) {
                    logger.warn(
                        "Trigger call failed ({}) but dbt Cloud already has a matching run {} (status {}); adopting it instead of failing",
                        e.getMessage(), adoptedRun.getId(), adoptedRun.getStatus()
                    );
                    return this.waitOrReturn(runContext, adoptedRun.getId());
                }
            }
            throw e;
        }

        RunResponse triggerRunResponse = triggerResponse.getBody();
        if (triggerRunResponse == null) {
            throw new IllegalStateException("Missing body on trigger");
        }

        logger.info("Job status {} with response: {}", triggerResponse.getStatus(), triggerRunResponse);

        return this.waitOrReturn(runContext, triggerRunResponse.getData().getId());
    }

    // Bounds the run-list lookup below to the most recent runs of the job. `order_by=-id` puts the
    // newest run first, so the tagged run is only missed if more than this many newer runs of the
    // same job were created since it started: a residual limitation, documented on the method itself.
    private static final int FIND_RUN_LOOKUP_LIMIT = 100;

    /**
     * Start-of-run reattach check (worker restart, task-level {@code retry}, manual restart from a
     * failed task). Adopts a run this taskrun already started only if it is still in flight (queued,
     * starting, running) or already succeeded, never one that ended in Error or Cancelled: a genuinely
     * failed dbt run must let the retry / restart re-trigger a fresh attempt instead of silently
     * re-adopting and re-reporting the same historical failure. A succeeded run is still adopted so a
     * delayed retry after a lost response does not create a duplicate.
     */
    private Run findReattachableRunForThisTaskRun(RunContext runContext) throws Exception {
        var run = this.findRunForThisTaskRun(runContext);
        if (run == null || run.getStatus() == JobStatus.NUMBER_20 || run.getStatus() == JobStatus.NUMBER_30) {
            return null;
        }
        return run;
    }

    /**
     * Look for a run of the job that this taskrun already started, matched strictly by the taskrun
     * tag in the run cause: that tag is the unique key, so any status (queued, running, or already
     * finished, whether success or failure) qualifies. Callers that need to only adopt an in-flight or
     * successful run (and let a genuine failure re-trigger) must filter the result themselves, e.g. via
     * {@link #findReattachableRunForThisTaskRun}. Returns null when there is none, in which case a
     * fresh run is triggered rather than attaching to a run this execution did not start.
     *
     * <p>
     * The lookup is bounded to the {@value #FIND_RUN_LOOKUP_LIMIT} most recent runs of the job
     * (newest first via {@code order_by=-id}), not paginated further. If the tagged run has been
     * pushed past that window by that many newer runs of the same job (e.g. after a long worker
     * downtime on a very actively scheduled job), it will not be found and a fresh run is triggered
     * instead, which can result in a duplicate in that rare case.
     */
    private Run findRunForThisTaskRun(RunContext runContext) throws Exception {
        HttpRequest.HttpRequestBuilder requestBuilder = HttpRequest.builder()
            .uri(
                URI.create(
                    runContext.render(this.baseUrl).as(String.class).orElseThrow() + "/api/v2/accounts/" + runContext.render(this.accountId).as(String.class).orElseThrow() +
                        "/runs/?job_definition_id=" + runContext.render(this.jobId).as(String.class).orElseThrow() +
                        "&order_by=-id" +
                        "&limit=" + FIND_RUN_LOOKUP_LIMIT +
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

    /**
     * After a trigger call fails ambiguously (the request may already have reached dbt Cloud), check
     * whether it actually created a run before giving up. A run that was just created can take a
     * moment to appear in the run list, so this retries a few times with a short pause rather than
     * concluding "no run" on the first empty result.
     */
    private Run confirmRunAfterAmbiguousFailure(RunContext runContext) throws Exception {
        for (int attempt = 1; attempt <= CONFIRM_LOOKUP_MAX_ATTEMPTS; attempt++) {
            var found = this.findRunForThisTaskRun(runContext);
            if (found != null) {
                return found;
            }
            if (attempt < CONFIRM_LOOKUP_MAX_ATTEMPTS) {
                try {
                    Thread.sleep(CONFIRM_LOOKUP_BACKOFF.toMillis());
                } catch (InterruptedException ie) {
                    // Stop looking rather than propagate: the caller must fall through to rethrow the
                    // original trigger failure, not fail on an interrupted backoff.
                    Thread.currentThread().interrupt();
                    return null;
                }
            }
        }
        return null;
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
