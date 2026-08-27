package io.kestra.plugin.dbt.cloud;

import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiPredicate;

import org.slf4j.Logger;

import io.kestra.core.http.HttpRequest;
import io.kestra.core.http.HttpResponse;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.retrys.Constant;
import io.kestra.core.runners.RunContext;
import io.kestra.core.utils.RetryUtils;
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

    // A just-created run can take a moment to show up in the run list, so the confirm lookup is retried.
    private static final int CONFIRM_LOOKUP_MAX_ATTEMPTS = 3;
    private static final Duration CONFIRM_LOOKUP_BACKOFF = Duration.ofSeconds(2);

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
            If true, the task reattaches to a run it already started (matched by the taskrun id in the run \
            cause) instead of triggering a duplicate. On a worker restart or retry it adopts an in-flight or \
            already-succeeded run, so a delayed retry after a lost response does not duplicate, but lets a \
            failed or cancelled run re-trigger. If the trigger call itself fails with an ambiguous error \
            (a read timeout, a mid-flight drop, or a 502/504 gateway error) that may mean dbt Cloud already \
            received it, it confirms and adopts the run this attempt created (never a stale run left over \
            from a prior attempt) and reports its real outcome. Two small residual gaps remain. A 503 on the \
            trigger POST is still retried internally, since a 503 almost always means the request never \
            reached dbt Cloud, so in a rare case it could double-trigger. And the restart or retry check \
            scans only the most recent runs of the job, so on a very busy or heavily shared job that has \
            accumulated a large backlog of runs since this one started, it may not find the earlier run and \
            could trigger a duplicate. Default false, which always triggers a new run."""
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

        // Baseline id of the newest tagged run seen before the POST below, used by the ambiguous-failure
        // confirm path to reject a stale run left over from a prior attempt (same taskrun id, reused tag).
        long baseline = 0L;
        if (reattachEnabled) {
            StartLookup lookup = this.findStartOfRunLookup(runContext);
            if (lookup.adoptableRun() != null) {
                logger.info(
                    "Reattached to run {} (status {}) already started by this taskrun instead of triggering a new one",
                    lookup.adoptableRun().getId(), lookup.adoptableRun().getStatus()
                );
                return this.waitOrReturn(runContext, lookup.adoptableRun().getId());
            }
            baseline = lookup.baseline();
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

        // The trigger POST is not idempotent, so when reattach is on it must not be blindly retried by the
        // generic HTTP layer on an ambiguous failure, since a retry could start the job twice. Keep the safe
        // transient retries (503, TLS handshake, refused connection) but let an ambiguous failure (read
        // timeout, mid-flight drop, or a 502/504 gateway error) surface here, where we can confirm and adopt
        // the run it may already have created. When reattach is off, behave exactly as before.
        BiPredicate<Throwable, String> triggerRetry = reattachEnabled
            ? (throwable, method) -> isRetriableTransientError(throwable, method) && !isAmbiguousFailure(throwable)
            : AbstractDbtCloud::isRetriableTransientError;

        HttpResponse<RunResponse> triggerResponse;
        try {
            triggerResponse = this.request(runContext, requestBuilder, RunResponse.class, triggerRetry);
        } catch (Exception e) {
            // An ambiguous failure may mean dbt already created the run, so confirm and adopt it before failing.
            // If no run was created the original failure is rethrown; a task-level retry then triggers a fresh
            // run and the start-of-run reattach above adopts it, so there is no in-task re-POST to duplicate.
            if (reattachEnabled && isAmbiguousFailure(e)) {
                Run adoptedRun = null;
                try {
                    adoptedRun = this.confirmRunAfterAmbiguousFailure(runContext, baseline);
                } catch (Exception confirmEx) {
                    // A failed confirm must not mask the original trigger failure.
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

    // Page size for the run-list lookup, newest first (order_by=-id).
    private static final int FIND_RUN_LOOKUP_LIMIT = 100;

    // Safety cap on how many pages the confirm lookup walks back before giving up, so a pathological
    // flood of runs cannot loop unbounded. FIND_RUN_LOOKUP_LIMIT times this is the deepest it looks.
    private static final int FIND_RUN_CONFIRM_MAX_PAGES = 10;

    // Pages the start-of-run reattach walks looking for an earlier run of this taskrun. Unlike the confirm
    // path it has no baseline to stop at, so on a first attempt (no earlier run) it walks up to this many
    // pages before giving up. Kept small so the cost on the common first-attempt path stays bounded.
    private static final int FIND_RUN_START_MAX_PAGES = 5;

    /**
     * Single start-of-run lookup, yielding both the adopt decision and the baseline for the later
     * ambiguous-failure confirm path. Adopts an in-flight or succeeded run (so a delayed retry after a
     * lost response does not duplicate), but never an Error or Cancelled one, so a genuine failure lets
     * the retry or restart re-trigger instead of re-reporting the old failure. The baseline is that same
     * run's id (0 if none), so a later confirm never mistakes it for a run created by this attempt.
     */
    private StartLookup findStartOfRunLookup(RunContext runContext) throws Exception {
        String tag = taskrunTag(runContext);
        long baseline = 0L;
        Run tagged = null;

        // Walk newest-first for an earlier run of this taskrun. Paginate so a busy job that has pushed the
        // earlier run past the first page does not silently miss it and trigger a duplicate. Bounded by
        // FIND_RUN_START_MAX_PAGES since, with no earlier run to find, there is no baseline to stop at.
        for (int page = 0; page < FIND_RUN_START_MAX_PAGES; page++) {
            List<Run> runs = this.fetchRunsPage(runContext, page * FIND_RUN_LOOKUP_LIMIT, 0L);
            if (runs.isEmpty()) {
                break;
            }
            if (page == 0) {
                // Baseline is the newest run id of the job (any status) seen before the POST, so a later
                // confirm only adopts a run created after it and never a stale run from a prior attempt.
                baseline = runs.stream().map(Run::getId).filter(id -> id != null).findFirst().orElse(0L);
            }
            tagged = runs.stream().filter(run -> matchesTaskrunTag(run, tag)).findFirst().orElse(null);
            if (tagged != null || runs.size() < FIND_RUN_LOOKUP_LIMIT) {
                break;
            }
        }

        // Adopt an in-flight or succeeded run, but never an Error or Cancelled one, so a genuine failure
        // lets the retry or restart re-trigger instead of re-reporting the old failure.
        boolean adoptable = tagged != null && tagged.getStatus() != JobStatus.NUMBER_20 && tagged.getStatus() != JobStatus.NUMBER_30;
        return new StartLookup(adoptable ? tagged : null, baseline);
    }

    private record StartLookup(Run adoptableRun, long baseline) {
    }

    /**
     * Find the run this attempt's POST created after an ambiguous failure. Matched by the taskrun tag and
     * required to be newer than {@code baseline} (the newest run seen before the POST), so a stale run
     * from a prior attempt is never mistaken for it. Pages newest-first until the run is found, or a page
     * crosses the baseline (every run newer than it has been seen, so none was created), or the page cap
     * is hit. Returns null in the not-found cases, and the caller then rethrows the original failure.
     */
    private Run findRunCreatedByThisTaskRun(RunContext runContext, long baseline) throws Exception {
        String tag = taskrunTag(runContext);
        for (int page = 0; page < FIND_RUN_CONFIRM_MAX_PAGES; page++) {
            List<Run> runs = this.fetchRunsPage(runContext, page * FIND_RUN_LOOKUP_LIMIT, baseline);
            if (runs.isEmpty()) {
                return null;
            }
            for (Run run : runs) {
                if (run.getId() != null && run.getId() <= baseline) {
                    // Reached the pre-POST baseline: a run this attempt created (id > baseline) would
                    // already have been seen, so there is none.
                    return null;
                }
                if (matchesTaskrunTag(run, tag)) {
                    return run;
                }
            }
            if (runs.size() < FIND_RUN_LOOKUP_LIMIT) {
                // Short page: the list is exhausted with no matching run.
                return null;
            }
        }
        runContext.logger().warn(
            "Confirm lookup walked {} pages without reaching baseline run id {}; a created run may have been missed",
            FIND_RUN_CONFIRM_MAX_PAGES, baseline
        );
        return null;
    }

    // One page of the job's runs, newest first (order_by=-id), from the given offset. When idGt > 0 the
    // dbt Cloud `id__gt` filter returns only runs newer than it, so the confirm lookup scans just the
    // runs created after its baseline instead of the whole newest-100, which is usually a single page.
    private List<Run> fetchRunsPage(RunContext runContext, int offset, long idGt) throws Exception {
        HttpRequest.HttpRequestBuilder requestBuilder = HttpRequest.builder()
            .uri(
                URI.create(
                    runContext.render(this.baseUrl).as(String.class).orElseThrow() + "/api/v2/accounts/" + runContext.render(this.accountId).as(String.class).orElseThrow() +
                        "/runs/?job_definition_id=" + runContext.render(this.jobId).as(String.class).orElseThrow() +
                        "&order_by=-id" +
                        "&limit=" + FIND_RUN_LOOKUP_LIMIT +
                        "&offset=" + offset +
                        (idGt > 0 ? "&id__gt=" + idGt : "") +
                        "&include_related=" + URLEncoder.encode("[\"trigger\"]", StandardCharsets.UTF_8)
                )
            )
            .method("GET");

        HttpResponse<RunListResponse> response = this.request(runContext, requestBuilder, RunListResponse.class);

        RunListResponse listResponse = response.getBody();
        if (listResponse == null || listResponse.getData() == null) {
            return List.of();
        }
        return listResponse.getData();
    }

    private static boolean matchesTaskrunTag(Run run, String tag) {
        return run.getTrigger() != null
            && run.getTrigger().getCause() != null
            && run.getTrigger().getCause().contains(tag);
    }

    /**
     * After an ambiguous trigger failure, check whether the run was created, retrying the paginated lookup
     * a few times since a just-created run can take a moment to appear in the run list. The baseline and
     * tag matching are handled by {@link #findRunCreatedByThisTaskRun}. Throws once the attempts are
     * exhausted without a match, which the caller treats as "not created".
     */
    private Run confirmRunAfterAmbiguousFailure(RunContext runContext, long baseline) throws Exception {
        return RetryUtils.<Run, Exception> of(
            Constant.builder()
                .interval(CONFIRM_LOOKUP_BACKOFF)
                .maxAttempts(CONFIRM_LOOKUP_MAX_ATTEMPTS)
                .build()
        )
            .run(
                // findRunCreatedByThisTaskRun already applies the baseline, so retry only while it has not
                // yet surfaced (a just-created run can lag in the run list for a moment).
                (run, throwable) -> throwable == null && run == null,
                () -> this.findRunCreatedByThisTaskRun(runContext, baseline)
            );
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
