package io.kestra.plugin.dbt.cloud;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;

import com.fasterxml.jackson.annotation.JsonIgnore;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.http.HttpRequest;
import io.kestra.core.http.client.HttpClientException;
import io.kestra.core.http.client.HttpClientResponseException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.storages.kv.KVMetadata;
import io.kestra.core.storages.kv.KVStore;
import io.kestra.core.storages.kv.KVValue;
import io.kestra.core.storages.kv.KVValueAndMetadata;
import io.kestra.core.utils.Await;
import io.kestra.core.utils.RetryUtils;
import io.kestra.plugin.dbt.ResultParser;
import io.kestra.plugin.dbt.cloud.models.JobStatus;
import io.kestra.plugin.dbt.cloud.models.JobStatusHumanizedEnum;
import io.kestra.plugin.dbt.cloud.models.ManifestArtifact;
import io.kestra.plugin.dbt.cloud.models.Run;
import io.kestra.plugin.dbt.cloud.models.RunListResponse;
import io.kestra.plugin.dbt.cloud.models.RunResponse;
import io.kestra.plugin.dbt.cloud.models.Step;
import io.kestra.plugin.dbt.models.RunResult;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.AssertTrue;
import lombok.*;
import lombok.experimental.SuperBuilder;

import static io.kestra.core.utils.Rethrow.throwSupplier;
import static java.lang.Math.max;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Monitor a dbt Cloud run",
    description = "Polls a dbt Cloud run until it ends, streaming step logs and downloading artifacts. Takes a `runId`, or a `jobId` or `environmentId` to read that job's or environment's most recent finished run so lineage stays fresh for runs Kestra did not trigger. Fails on non-successful statuses unless `failOnUnsuccessful` is false. When resolving by `jobId` or `environmentId`, a run that was already read is skipped, so a scheduled refresh does not repeat itself. Defaults to 5s polling and a 60m timeout, and can parse run results for node timings."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            code = """
                id: dbt_check_status
                namespace: company.team

                tasks:
                  - id: check_status
                    type: io.kestra.plugin.dbt.cloud.CheckStatus
                    accountId: "12345"
                    token: "{{ secret('DBT_TOKEN') }}"
                    runId: "98765"
                """
        ),
        @Example(
            title = "Refresh lineage on a schedule from the most recent finished run of a dbt Cloud job, including runs Kestra did not trigger.",
            full = true,
            code = """
                id: dbt_lineage_refresh
                namespace: company.team

                triggers:
                  - id: hourly
                    type: io.kestra.plugin.core.trigger.Schedule
                    cron: "0 * * * *"

                tasks:
                  - id: refresh_lineage
                    type: io.kestra.plugin.dbt.cloud.CheckStatus
                    accountId: "12345"
                    token: "{{ secret('DBT_TOKEN') }}"
                    jobId: "4321"
                    failOnUnsuccessful: false
                """
        )
    }
)
public class CheckStatus extends AbstractDbtCloud implements RunnableTask<CheckStatus.Output> {
    private static final Set<JobStatus> ENDED_STATUS = Set.of(
        JobStatus.NUMBER_10, // Success
        JobStatus.NUMBER_20, // Error
        JobStatus.NUMBER_30 // Cancelled
    );

    // Prefix for the already-processed watermark, so the key is recognisable in the namespace KV UI.
    private static final String PROCESSED_KEY_PREFIX = "dbt-cloud-last-run";

    // Separator between the parts of that key. Dots are stripped from every part below, so a part can
    // never contain one: without that, flow "a_b" + task "c" and flow "a" + task "b_c" would collide.
    private static final String PROCESSED_KEY_SEPARATOR = ".";

    // KV keys allow alphanumerics, dots, underscores and hyphens. Dots are excluded here because they
    // are the separator.
    private static final Pattern UNSAFE_KEY_CHARS = Pattern.compile("[^a-zA-Z0-9_-]");

    @Schema(
        title = "Run ID",
        description = "dbt Cloud run identifier to monitor. Mutually exclusive with `jobId` and `environmentId`."
    )
    @PluginProperty(group = "main")
    Property<String> runId;

    @Schema(
        title = "Job ID",
        description = "dbt Cloud job identifier. Reads that job's most recent finished run instead of a run this flow started, "
            + "so lineage can be refreshed for runs triggered outside Kestra. Pair with `failOnUnsuccessful: false` when the "
            + "resolved run may have failed. Mutually exclusive with `runId` and `environmentId`."
    )
    @PluginProperty(group = "main")
    Property<String> jobId;

    @Schema(
        title = "Environment ID",
        description = "dbt Cloud environment identifier. Reads the most recent finished run anywhere in that environment, "
            + "whichever job produced it. dbt writes `manifest.json` at parse time so it describes the whole project, which "
            + "means any run in the environment refreshes the full lineage graph and several jobs give more chances at a "
            + "fresh one than a single job does. Mutually exclusive with `runId` and `jobId`."
    )
    @PluginProperty(group = "main")
    Property<String> environmentId;

    @AssertTrue(message = "Exactly one of 'runId', 'jobId' or 'environmentId' must be provided.")
    @JsonIgnore
    public boolean isValidRunSelector() {
        return Stream.of(runId, jobId, environmentId).filter(Objects::nonNull).count() == 1;
    }

    @Schema(
        title = "Poll frequency",
        description = "Interval between status checks while waiting. Default 5s."
    )
    @Builder.Default
    Property<Duration> pollFrequency = Property.ofValue(Duration.ofSeconds(5));

    @Schema(
        title = "Max wait duration",
        description = "Upper bound for waiting on completion. Default 60m."
    )
    @Builder.Default
    Property<Duration> maxDuration = Property.ofValue(Duration.ofMinutes(60));

    @Builder.Default
    @Schema(
        title = "Parse run results",
        description = "If true (default), parses `run_results.json` to expose node timings; otherwise uploads the artifact as-is."
    )
    @PluginProperty(group = "advanced")
    protected Property<Boolean> parseRunResults = Property.ofValue(Boolean.TRUE);

    @Builder.Default
    @Schema(
        title = "Fail if the run ends in a non-successful state",
        description = "When true (default), a run ending in `Error` or `Cancelled` raises a task failure. "
            + "Set to false to read a finished run without failing the task, which a scheduled lineage refresh needs "
            + "since it does not own the run it reads. Left true, a `jobId` or `environmentId` refresh fails once for a "
            + "given failed run and then skips it, rather than failing on every tick."
    )
    @PluginProperty(group = "reliability")
    Property<Boolean> failOnUnsuccessful = Property.ofValue(Boolean.TRUE);

    @Builder.Default
    @Getter(AccessLevel.NONE)
    private transient List<JobStatusHumanizedEnum> loggedStatus = new ArrayList<>();

    @Builder.Default
    @Getter(AccessLevel.NONE)
    private transient Map<Long, Long> loggedSteps = new HashMap<>();

    @Override
    public CheckStatus.Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();

        // One monitoring session per invocation. The dedup guard makes repeated run() calls on a single
        // instance a designed-for path, and carrying these over would suppress a later run's status logs.
        this.loggedStatus = new ArrayList<>();
        this.loggedSteps = new HashMap<>();

        Long runIdRendered = resolveRunId(runContext);

        // Null unless the guard applies, so the runId path and TriggerRun never touch the KV store.
        String processedKey = alreadyProcessedKey(runContext);

        // A dbt run's artifacts are immutable, so lineage for a given run id only ever needs emitting once.
        // The execution still does all of its work and returns its usual outputs: only the emit is skipped,
        // so a repeated refresh adds no duplicate lineage events (issue #318) without turning into a no-op.
        boolean alreadyEmitted = processedKey != null && isAlreadyProcessed(runContext, processedKey, runIdRendered);
        if (alreadyEmitted) {
            logger.info("Lineage for run '{}' was already emitted by this task, reading it without re-emitting", runIdRendered);
        }

        // wait for end
        RunResponse finalRunResponse = Await.until(
            throwSupplier(() ->
            {
                Optional<RunResponse> fetchRunResponse;
                try {
                    fetchRunResponse = fetchRunResponse(runContext, runIdRendered, false);
                } catch (Exception e) {
                    // Failing to read the status is not a run failure: the run keeps executing on dbt
                    // Cloud, so poll again on the next cycle (bounded by maxDuration). Non-transient
                    // errors (e.g. 401/403/404, bad config) never resolve by waiting, so fail fast.
                    if (isTransientReadFailure(e)) {
                        logger.warn("Could not read run '{}' status, retrying on next poll: {}", runIdRendered, e.getMessage());
                        return null;
                    }
                    throw e;
                }

                if (fetchRunResponse.isPresent()) {
                    logSteps(logger, fetchRunResponse.get());

                    var data = fetchRunResponse.get().getData();

                    if (data.getStatus() == null && data.getIsComplete() == null && data.getStatusHumanized() == null) {
                        logger.warn("Received response with no status indicator from dbt Cloud — skipping this poll cycle");
                    } else if (isEnded(data)) {
                        return fetchRunResponse.get();
                    }
                }

                return null;
            }),
            runContext.render(this.pollFrequency).as(Duration.class).orElseThrow(),
            runContext.render(this.maxDuration).as(Duration.class).orElseThrow()
        );

        // Best-effort debug=true fetch for fuller step logs; truncated_debug_logs population timing
        // isn't part of dbt Cloud's terminal-run contract, so a failure here must not fail the run.
        try {
            var debugRunResponse = fetchRunResponse(runContext, runIdRendered, true);
            if (debugRunResponse.isPresent()) {
                finalRunResponse = debugRunResponse.get();
            }
        } catch (IllegalVariableEvaluationException | HttpClientException | IOException e) {
            logger.debug("Unable to fetch final debug logs for run '{}' — falling back to logs collected during polling", runIdRendered, e);
        }

        // final response
        logSteps(logger, finalRunResponse);

        boolean successful = isSuccessful(finalRunResponse.getData());

        // Download and parse artifacts before failing on a non-successful run: dbt Cloud saves
        // run_results.json for failed runs too, and parseRunResult is what emits the per-model
        // dynamic taskruns/logs (issue #315) — those must land even though the task ends up throwing.
        URI runResultsUri = null;
        URI manifestUri = null;
        List<String> assets = List.of();
        boolean artifactsProcessed = false;
        // False until a manifest is actually read and emitted, so a run whose artifacts have not been
        // uploaded yet (dbt Cloud writes them asynchronously) is retried rather than recorded as done.
        boolean lineageLanded = false;
        try {
            // Artifacts are uploaded asynchronously by dbt Cloud and manifest.json is absent for some
            // run shapes (e.g. dbt source freshness). Tolerate 404 so a legitimate success is not
            // reported as a failure.
            Path runResultsArtifact = downloadArtifacts(runContext, runIdRendered, "run_results.json", RunResult.class);
            Path manifestArtifact = downloadArtifacts(runContext, runIdRendered, "manifest.json", ManifestArtifact.class);

            io.kestra.plugin.dbt.models.Manifest manifest = null;
            if (manifestArtifact != null) {
                ResultParser.ManifestResult manifestResult = ResultParser.parseManifestWithAssets(runContext, manifestArtifact.toFile(), !alreadyEmitted);
                manifest = manifestResult.manifest();
                manifestUri = manifestResult.uri();
                assets = manifestResult.assetIds();
                // Only a complete emit means this run is done. A partial one leaves no watermark so the
                // next execution retries it, and assets upsert by id so the ones that landed do not double.
                lineageLanded = manifestResult.fullyEmitted();
            }

            if (runResultsArtifact != null) {
                if (runContext.render(this.parseRunResults).as(Boolean.class).orElse(false)) {
                    runResultsUri = ResultParser.parseRunResult(runContext, runResultsArtifact.toFile(), manifest, !alreadyEmitted);
                } else {
                    runResultsUri = runContext.storage().putFile(runResultsArtifact.toFile());
                }
            }

            artifactsProcessed = true;
        } catch (Exception e) {
            if (successful) {
                // On a successful run, a broken artifact download/parse is the only failure and must surface.
                throw e;
            }
            logger.warn("Unable to download or parse artifacts for failed run '{}': {}", runIdRendered, e.getMessage(), e);
        }

        // Recorded here, before the failure throw below, because the lineage for this run has already been
        // emitted: a later execution resolving the same failed run must not emit it a second time. Gated on
        // the lineage having actually landed in full, so a missing manifest, an unreadable one, or a partial
        // emit is retried on the next execution instead of being skipped forever.
        if (artifactsProcessed && lineageLanded) {
            rememberProcessed(runContext, processedKey, runIdRendered);
        }

        if (!successful) {
            String failure = "Failed run with status '" + finalRunResponse.getData().getStatusHumanized() +
                "' after " + finalRunResponse.getData().getDurationHumanized() +
                (finalRunResponse.getData().getStatusMessage() != null
                    ? ": " + finalRunResponse.getData().getStatusMessage()
                    : "")
                +
                ": " + finalRunResponse;

            if (runContext.render(this.failOnUnsuccessful).as(Boolean.class).orElse(Boolean.TRUE)) {
                throw new Exception(failure);
            }

            // Artifacts and lineage were already emitted above, so reporting the status and returning
            // normally leaves the caller the run's data without failing a run this task does not own.
            logger.warn("{}", failure);
        }

        return Output.builder()
            .runId(runIdRendered)
            .lineageEmitted(!alreadyEmitted && lineageLanded)
            .assets(assets)
            .runResults(runResultsUri)
            .manifest(manifestUri)
            .build();
    }

    /**
     * The run to read: either the explicit {@code runId}, or the most recent finished run of the given
     * job or environment. Those paths exist so lineage can be refreshed for runs Kestra did not trigger,
     * where no run id is known up front.
     */
    private Long resolveRunId(RunContext runContext) throws Exception {
        if (this.runId != null) {
            return Long.parseLong(runContext.render(this.runId).as(String.class).orElseThrow());
        }

        return findLatestFinishedRun(runContext);
    }

    /**
     * Newest finished run of the job. Restricted to terminal statuses because dbt Cloud only writes
     * artifacts once a run ends, so an in-flight run would turn this fetch into an open-ended wait.
     * Failed and cancelled runs are kept: dbt writes the manifest at parse time, so it describes the
     * project just as accurately as a successful run's does, and skipping them would discard the
     * fresher answer for no gain. Ordering by {@code -finished_at} with {@code limit=1} makes dbt Cloud
     * do the selection, so only the one run being read is fetched.
     */
    private Long findLatestFinishedRun(RunContext runContext) throws Exception {
        RunSelector selector = runSelector(runContext);

        HttpRequest.HttpRequestBuilder requestBuilder = HttpRequest.builder()
            .uri(
                URI.create(
                    runContext.render(this.baseUrl).as(String.class).orElseThrow()
                        + "/api/v2/accounts/" + runContext.render(this.accountId).as(String.class).orElseThrow()
                        + "/runs/?" + selector.queryParam() + "=" + URLEncoder.encode(selector.value(), StandardCharsets.UTF_8)
                        + "&status__in=" + URLEncoder.encode(endedStatusFilter(), StandardCharsets.UTF_8)
                        + "&order_by=" + URLEncoder.encode("-finished_at", StandardCharsets.UTF_8)
                        + "&limit=1"
                )
            )
            .method("GET");

        RunListResponse response = this.request(runContext, requestBuilder, RunListResponse.class).getBody();

        if (response == null || response.getData() == null || response.getData().isEmpty()) {
            throw new IllegalStateException(
                "No finished run found for dbt Cloud " + selector.describe() + ". " +
                    "It may never have run to completion, or the token may not have access to it."
            );
        }

        Long resolved = response.getData().getFirst().getId();
        if (resolved == null) {
            throw new IllegalStateException("dbt Cloud returned a run without an id for " + selector.describe());
        }

        runContext.logger().info("Resolved run '{}' as the most recent finished run of {}", resolved, selector.describe());
        return resolved;
    }

    /** Which dbt Cloud filter narrows the run list, and to what. Null when an explicit runId is given. */
    private RunSelector runSelector(RunContext runContext) throws IllegalVariableEvaluationException {
        if (this.jobId != null) {
            return new RunSelector("job_definition_id", runContext.render(this.jobId).as(String.class).orElseThrow());
        }

        if (this.environmentId != null) {
            return new RunSelector("environment_id", runContext.render(this.environmentId).as(String.class).orElseThrow());
        }

        return null;
    }

    private record RunSelector(String queryParam, String value) {
        String describe() {
            return ("job_definition_id".equals(queryParam) ? "job '" : "environment '") + value + "'";
        }
    }

    /**
     * KV key holding the last run this task processed, or null when the guard does not apply: an explicit
     * {@code runId} (the caller named one specific run to read), or no flow context to scope the key with.
     * To force a re-read, give the run id directly or delete the key from the namespace KV store. Scoped to flow, task and selector so two refreshers in one namespace,
     * or one task iterating over several jobs, never share a watermark. The store is namespace-scoped
     * already, so the namespace is not repeated in the key.
     */
    private String alreadyProcessedKey(RunContext runContext) {
        if (this.jobId == null && this.environmentId == null) {
            return null;
        }

        try {
            RunContext.FlowInfo flowInfo = runContext.flowInfo();
            if (flowInfo == null || flowInfo.id() == null) {
                return null;
            }

            RunSelector selector = runSelector(runContext);

            return String.join(
                PROCESSED_KEY_SEPARATOR,
                PROCESSED_KEY_PREFIX,
                safeKeyPart(flowInfo.id()),
                safeKeyPart(this.getId()),
                safeKeyPart(selector.queryParam()),
                safeKeyPart(selector.value())
            );
        } catch (Exception e) {
            // No flow context (or an unrenderable property) only means the guard cannot be scoped. Doing
            // the work again is the safe outcome, so fall through rather than failing the refresh.
            runContext.logger().debug("Could not build the already-processed key, running without the skip guard", e);
            return null;
        }
    }

    /** True when this task last processed the same run. A KV read failure reports false, so the work is redone rather than lost. */
    private boolean isAlreadyProcessed(RunContext runContext, String key, Long runId) {
        try {
            KVStore kv = runContext.namespaceKv(runContext.flowInfo().namespace());
            Optional<KVValue> stored = kv.getValue(key);

            return stored
                .map(KVValue::value)
                .map(String::valueOf)
                .filter(value -> value.equals(String.valueOf(runId)))
                .isPresent();
        } catch (Exception e) {
            runContext.logger().warn("Could not read the already-processed marker '{}', re-reading run '{}'", key, runId, e);
            return false;
        }
    }

    /** Records the run as processed. A write failure only costs a duplicate on the next tick, so it must not fail a run already read. */
    private void rememberProcessed(RunContext runContext, String key, Long runId) {
        if (key == null) {
            return;
        }

        try {
            KVStore kv = runContext.namespaceKv(runContext.flowInfo().namespace());
            kv.put(
                key,
                new KVValueAndMetadata(
                    new KVMetadata("Last dbt Cloud run read by this task, used to skip an unchanged run.", (Duration) null),
                    String.valueOf(runId)
                )
            );
        } catch (Exception e) {
            runContext.logger().warn("Could not record run '{}' as processed under '{}'", runId, key, e);
        }
    }

    private static String safeKeyPart(String value) {
        return value == null ? "" : UNSAFE_KEY_CHARS.matcher(value).replaceAll("_");
    }

    /** dbt Cloud's `status__in` value for the terminal statuses, derived from {@link #ENDED_STATUS} so the two cannot drift. */
    private static String endedStatusFilter() {
        return ENDED_STATUS.stream()
            .map(JobStatus::toString)
            .sorted()
            .collect(Collectors.joining(",", "[", "]"));
    }

    // Precedence: integer status → is_complete → status_humanized
    private boolean isEnded(Run data) {
        if (data.getStatus() != null) {
            return ENDED_STATUS.contains(data.getStatus());
        }
        if (data.getIsComplete() != null) {
            return Boolean.TRUE.equals(data.getIsComplete());
        }
        if (data.getStatusHumanized() != null) {
            return data.getStatusHumanized() == JobStatusHumanizedEnum.SUCCESS
                || data.getStatusHumanized() == JobStatusHumanizedEnum.ERROR
                || data.getStatusHumanized() == JobStatusHumanizedEnum.CANCELLED;
        }
        return false;
    }

    // Precedence: integer status → is_success/is_error → status_humanized
    private boolean isSuccessful(Run data) {
        if (data.getStatus() != null) {
            return data.getStatus() == JobStatus.NUMBER_10;
        }
        if (data.getIsSuccess() != null) {
            return Boolean.TRUE.equals(data.getIsSuccess()) && !Boolean.TRUE.equals(data.getIsError());
        }
        return JobStatusHumanizedEnum.SUCCESS.equals(data.getStatusHumanized());
    }

    /**
     * Whether a failed status read is transient, meaning polling should continue rather than fail the
     * task. The run keeps executing on dbt Cloud while we cannot read its status, so a transient read
     * failure is not a run failure. Non-transient errors (e.g. 401/403/404, bad config) never resolve
     * by waiting, so they propagate and the task fails fast.
     */
    static boolean isTransientReadFailure(Throwable e) {
        // request() surfaces an exhausted retry as RetryFailed wrapping the last error, so unwrap it.
        Throwable cause = e instanceof RetryUtils.RetryFailed && e.getCause() != null ? e.getCause() : e;
        return isRetriableTransientError(cause, "GET");
    }

    private void logSteps(Logger logger, RunResponse runResponse) {
        // status changed
        if (!loggedStatus.contains(runResponse.getData().getStatusHumanized())) {
            logger.debug(
                "Status changed to '{}' after {}",
                runResponse.getData().getStatusHumanized(),
                runResponse.getData().getDurationHumanized()
            );
            loggedStatus.add(runResponse.getData().getStatusHumanized());
        }

        // log steps
        for (Step step : runResponse.getData().getRunSteps()) {
            if (!step.getLogs().isEmpty()) {
                if (!loggedSteps.containsKey(step.getId())) {
                    loggedSteps.put(step.getId(), 0L);
                }

                if (step.getLogs().length() > loggedSteps.get(step.getId())) {
                    for (String s : step.getLogs().substring((int) max(loggedSteps.get(step.getId()) - 1L, 0L)).split("\n")) {
                        logger.info("[Step {}]: {}", step.getName(), s);
                    }
                    loggedSteps.put(step.getId(), (long) step.getLogs().length());
                }
            }
        }
    }

    private Optional<RunResponse> fetchRunResponse(RunContext runContext, Long id, Boolean debug) throws IllegalVariableEvaluationException, HttpClientException, IOException {
        HttpRequest.HttpRequestBuilder requestBuilder = HttpRequest.builder()
            .uri(
                URI.create(
                    runContext.render(this.baseUrl).as(String.class).orElseThrow() + "/api/v2/accounts/" + runContext.render(this.accountId).as(String.class).orElseThrow() + "/runs/" + id +
                        "/?include_related=" + URLEncoder.encode("[\"trigger\",\"job\"" + (debug ? ",\"debug_logs\"" : "") + ",\"run_steps\",\"environment\"]", StandardCharsets.UTF_8)
                )
            )
            .method("GET");

        return Optional.ofNullable(this.request(runContext, requestBuilder, RunResponse.class).getBody());
    }

    /**
     * Downloads an artifact and writes it to a temp file. Returns null when the artifact is not
     * found (404), which is a legitimate outcome for async uploads or run shapes that don't
     * produce every artifact (e.g. manifest.json is absent for dbt source freshness runs).
     * 5xx errors are still retried by {@link AbstractDbtCloud#request}; other unexpected errors
     * still propagate.
     */
    private <T> Path downloadArtifacts(RunContext runContext, Long runId, String path, Class<T> responseType)
        throws IllegalVariableEvaluationException, IOException, HttpClientException {
        var requestBuilder = HttpRequest.builder()
            .uri(
                URI.create(
                    runContext.render(this.baseUrl).as(String.class).orElseThrow()
                        + "/api/v2/accounts/" + runContext.render(this.accountId).as(String.class).orElseThrow()
                        + "/runs/" + runId + "/artifacts/" + path
                )
            )
            .method("GET");

        T artifact;
        try {
            artifact = this.request(runContext, requestBuilder, responseType).getBody();
        } catch (HttpClientResponseException ex) {
            if (ex.getResponse().getStatus().getCode() == 404) {
                runContext.logger().debug("Artifact '{}' not found (404) — skipping", path);
                return null;
            }
            throw ex;
        }

        var artifactJson = JacksonMapper.ofJson().writeValueAsString(artifact);
        var tempFile = runContext.workingDir().createTempFile(".json");
        Files.writeString(tempFile, artifactJson, StandardOpenOption.TRUNCATE_EXISTING);
        return tempFile;
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Run ID",
            description = "Identifier of the dbt Cloud run that was read. Useful when it was resolved from `jobId`."
        )
        private Long runId;

        @Schema(
            title = "Lineage emitted",
            description = "True when this execution emitted the run's asset lineage. False when the same run had "
                + "already been emitted, so only the emit was skipped and the run was still read."
        )
        private boolean lineageEmitted;

        @Schema(
            title = "Assets",
            description = "Asset ids described by the run's manifest, reported whether or not lineage was emitted."
        )
        private List<String> assets;

        @Schema(
            title = "Run results URI",
            description = "Internal storage URI for the downloaded `run_results.json`, when present."
        )
        private URI runResults;

        @Schema(
            title = "Manifest URI",
            description = "Internal storage URI for the downloaded `manifest.json`, when present."
        )
        private URI manifest;
    }
}
