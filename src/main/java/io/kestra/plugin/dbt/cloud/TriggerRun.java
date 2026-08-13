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
            from a prior attempt) and reports its real outcome. A small residual gap remains: a 503 on the \
            trigger POST is still retried internally, since a 503 almost always means the request never \
            reached dbt Cloud, so in a rare case it could double-trigger. Default false, which always \
            triggers a new run."""
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

    // Newest first, so the tagged run is only missed if this many newer runs of the job appeared since.
    private static final int FIND_RUN_LOOKUP_LIMIT = 100;

    /**
     * Single start-of-run lookup, yielding both the adopt decision and the baseline for the later
     * ambiguous-failure confirm path. Adopts an in-flight or succeeded run (so a delayed retry after a
     * lost response does not duplicate), but never an Error or Cancelled one, so a genuine failure lets
     * the retry or restart re-trigger instead of re-reporting the old failure. The baseline is that same
     * run's id (0 if none), so a later confirm never mistakes it for a run created by this attempt.
     */
    private StartLookup findStartOfRunLookup(RunContext runContext) throws Exception {
        var run = this.findRunForThisTaskRun(runContext);
        long baseline = (run != null && run.getId() != null) ? run.getId() : 0L;
        boolean adoptable = run != null && run.getStatus() != JobStatus.NUMBER_20 && run.getStatus() != JobStatus.NUMBER_30;
        return new StartLookup(adoptable ? run : null, baseline);
    }

    private record StartLookup(Run adoptableRun, long baseline) {
    }

    /**
     * Find a run this taskrun started, matched by its tag in the run cause, in any status. Callers that
     * must not adopt a failed run filter via {@link #findStartOfRunLookup}. Bounded to the
     * {@value #FIND_RUN_LOOKUP_LIMIT} newest runs of the job, not paginated further.
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
     * After an ambiguous trigger failure, check whether the run was created, retrying the lookup a few
     * times since a just-created run can take a moment to appear in the run list. Only a run whose id is
     * strictly greater than {@code baseline} is accepted: the tag in the run cause is reused across
     * retries of the same taskrun, so the newest tagged run could still be a stale run from a prior
     * attempt (id == baseline) rather than the one this attempt's POST just created. Throws once the
     * attempts are exhausted without such a run, which the caller treats as "not created".
     */
    private Run confirmRunAfterAmbiguousFailure(RunContext runContext, long baseline) throws Exception {
        return RetryUtils.<Run, Exception> of(
            Constant.builder()
                .interval(CONFIRM_LOOKUP_BACKOFF)
                .maxAttempts(CONFIRM_LOOKUP_MAX_ATTEMPTS)
                .build()
        )
            .run(
                (run, throwable) -> throwable == null && (run == null || run.getId() == null || run.getId() <= baseline),
                () -> this.findRunForThisTaskRun(runContext)
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
