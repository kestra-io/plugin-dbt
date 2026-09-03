package io.kestra.plugin.dbt.cloud;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.*;
import java.util.HexFormat;
import java.util.regex.Pattern;
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
import io.kestra.plugin.dbt.cloud.models.Job;
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
    description = """
        Polls a dbt Cloud run until it ends, streaming step logs and downloading artifacts.
        Takes a `runId`, or a `jobId` or `environmentId` to read the most recent successful run of that job or
        environment, which keeps lineage fresh for runs Kestra did not trigger.
        Fails on non-successful statuses unless `failOnUnsuccessful` is false.
        Defaults to 5s polling and a 60m timeout.
        """
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
                    assets:
                      enableAuto: true
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

    private static final String PROCESSED_KEY_PREFIX = "dbt-cloud-last-run";

    // Dots are stripped from each key part so a part can never contain one, otherwise flow "a_b" with
    // task "c" would collide with flow "a" with task "b_c".
    private static final String PROCESSED_KEY_SEPARATOR = ".";
    private static final Pattern UNSAFE_KEY_CHARS = Pattern.compile("[^a-zA-Z0-9_-]");

    @Schema(
        title = "Run ID",
        description = """
            dbt Cloud run identifier to monitor.
            Mutually exclusive with `jobId` and `environmentId`.
            """
    )
    @PluginProperty(group = "main")
    Property<String> runId;

    @Schema(
        title = "Job ID",
        description = """
            dbt Cloud job identifier. Reads that job's most recent successful run, which refreshes lineage for
            runs triggered outside Kestra.
            Mutually exclusive with `runId` and `environmentId`.
            """
    )
    @PluginProperty(group = "main")
    Property<String> jobId;

    @Schema(
        title = "Environment ID",
        description = """
            dbt Cloud environment identifier. Reads the most recent successful run anywhere in that environment,
            whichever job produced it. dbt's manifest covers the whole project, so any run refreshes the full graph.
            Mutually exclusive with `runId` and `jobId`.
            """
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
        description = """
            When true (default), a run ending in `Error` or `Cancelled` raises a task failure.
            Set to false to read a run without failing the task, which matters when a `runId` is pinned on a
            schedule. `jobId` and `environmentId` resolve successful runs only, so it does not apply there.
            """
    )
    @PluginProperty(group = "reliability")
    Property<Boolean> failOnUnsuccessful = Property.ofValue(Boolean.TRUE);

    @Builder.Default
    @Getter(AccessLevel.NONE)
    @ToString.Exclude
    @EqualsAndHashCode.Exclude
    private transient List<JobStatusHumanizedEnum> loggedStatus = new ArrayList<>();

    @Builder.Default
    @Getter(AccessLevel.NONE)
    @ToString.Exclude
    @EqualsAndHashCode.Exclude
    private transient Map<Long, Long> loggedSteps = new HashMap<>();

    @Override
    public CheckStatus.Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();

        // Fresh per invocation: run() is called more than once on one instance, and stale entries here
        // would suppress a later run's status logs.
        this.loggedStatus = new ArrayList<>();
        this.loggedSteps = new HashMap<>();

        Long runIdRendered = resolveRunId(runContext);

        String processedKey = alreadyProcessedKey(runContext);

        // A run's artifacts are immutable, so its lineage only needs emitting once (issue #318). Everything
        // else still runs, so the outputs are the same on every tick.
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
        boolean lineageLanded = false;
        try {
            // Artifacts are uploaded asynchronously by dbt Cloud and manifest.json is absent for some
            // run shapes (e.g. dbt source freshness). Tolerate 404 so a legitimate success is not
            // reported as a failure.
            Path runResultsArtifact = downloadArtifacts(runContext, runIdRendered, "run_results.json", RunResult.class);
            Path manifestArtifact = downloadArtifacts(runContext, runIdRendered, "manifest.json", ManifestArtifact.class);

            io.kestra.plugin.dbt.models.Manifest manifest = null;
            if (manifestArtifact != null) {
                ResultParser.ManifestResult manifestResult = ResultParser.parseManifestWithAssets(
                    runContext, manifestArtifact.toFile(), !alreadyEmitted, producerMetadata(finalRunResponse.getData())
                );
                manifest = manifestResult.manifest();
                manifestUri = manifestResult.uri();
                assets = manifestResult.assetIds();
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

        // Gated on the lineage having landed in full, so a missing manifest or a partial emit is retried
        // on the next execution rather than skipped for good.
        if (!alreadyEmitted && artifactsProcessed && lineageLanded) {
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

            // Artifacts and lineage already landed above, so the caller keeps the run's data.
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
     * Facts about the producing dbt Cloud job, merged into every asset the run emits (issue #323). Freshness
     * needs a cadence, and for a job on dbt Cloud's own schedule the only source of it is the job itself.
     * The run response already carries the job, so this costs no extra call.
     *
     * A job whose schedule trigger is off is reported as such rather than as having no schedule, since a
     * disabled cron is not a cadence and must not be mistaken for one.
     */
    private static Map<String, Object> producerMetadata(Run run) {
        if (run == null || run.getJob() == null) {
            return Map.of();
        }

        Job job = run.getJob();
        Map<String, Object> metadata = new HashMap<>();

        if (job.getId() != null) {
            metadata.put("dbtCloudJobId", String.valueOf(job.getId()));
        }
        if (hasText(job.getName())) {
            metadata.put("dbtCloudJobName", job.getName());
        }

        boolean scheduled = job.getTriggers() != null && Boolean.TRUE.equals(job.getTriggers().getSchedule());
        String cron = job.getSchedule() == null ? null : job.getSchedule().getCron();

        // Both, per issue #323: the cron so it is visible, and the flag so a disabled schedule is not read
        // as a cadence. Dropping the cron when the trigger was off made the feature look like a no-op.
        if (hasText(cron)) {
            metadata.put("dbtCloudJobSchedule", cron);
        }
        if (job.getTriggers() != null) {
            metadata.put("dbtCloudJobScheduled", scheduled);
        }

        return metadata;
    }

    private static boolean hasText(String value) {
        return value != null && !value.isBlank();
    }

    /** The explicit {@code runId}, or the most recent successful run of the given job or environment. */
    private Long resolveRunId(RunContext runContext) throws Exception {
        if (this.runId != null) {
            return Long.parseLong(runContext.render(this.runId).as(String.class).orElseThrow());
        }

        return findLatestSuccessfulRun(runContext);
    }

    /**
     * Successful runs only. A failed run's manifest is usually complete, since dbt writes it at parse time,
     * but a run that dies before parsing has no manifest at all, and resolving one would leave the task with
     * nothing to emit and empty outputs until dbt produced another run.
     */
    private Long findLatestSuccessfulRun(RunContext runContext) throws Exception {
        RunSelector selector = runSelector(runContext);

        HttpRequest.HttpRequestBuilder requestBuilder = HttpRequest.builder()
            .uri(
                URI.create(
                    runContext.render(this.baseUrl).as(String.class).orElseThrow()
                        + "/api/v2/accounts/" + runContext.render(this.accountId).as(String.class).orElseThrow()
                        + "/runs/?" + selector.queryParam() + "=" + URLEncoder.encode(selector.value(), StandardCharsets.UTF_8)
                        + "&status=" + JobStatus.NUMBER_10
                        + "&order_by=" + URLEncoder.encode("-finished_at", StandardCharsets.UTF_8)
                        + "&limit=1"
                )
            )
            .method("GET");

        RunListResponse response = this.request(runContext, requestBuilder, RunListResponse.class).getBody();

        if (response == null || response.getData() == null || response.getData().isEmpty()) {
            throw new IllegalStateException(
                "No successful run found for dbt Cloud " + selector.describe() + ". " +
                    "It may never have completed successfully, or the token may not have access to it."
            );
        }

        Long resolved = response.getData().getFirst().getId();
        if (resolved == null) {
            throw new IllegalStateException("dbt Cloud returned a run without an id for " + selector.describe());
        }

        runContext.logger().info("Resolved run '{}' as the most recent successful run of {}", resolved, selector.describe());
        return resolved;
    }

    /** Which dbt Cloud filter narrows the run list, and to what. */
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
     * KV key for the last run this task emitted, or null when an explicit {@code runId} was given or there is
     * no flow context. Scoped to flow, task and selector so two refreshers never share a watermark. To force
     * a re-emit, pass the run id directly or delete the key.
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
            String[] parts = { flowInfo.id(), this.getId(), selector.queryParam(), selector.value() };

            return String.join(
                PROCESSED_KEY_SEPARATOR,
                PROCESSED_KEY_PREFIX,
                safeKeyPart(parts[0]),
                safeKeyPart(parts[1]),
                safeKeyPart(parts[2]),
                safeKeyPart(parts[3]),
                fingerprint(parts)
            );
        } catch (Exception e) {
            // Without a scoped key the safe outcome is to emit again, not to fail the refresh.
            runContext.logger().debug("Could not build the already-processed key, running without the skip guard", e);
            return null;
        }
    }

    /** A read failure reports false, so the lineage is re-emitted rather than lost. */
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

    /** A write failure only costs a duplicate next tick, so it must not fail a run already read. */
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

    /**
     * Short digest of the raw parts, appended so two tuples that sanitise to the same text still get
     * distinct keys. Substitution is many-to-one, and length-prefixing does not fix that: "a.b" and "a_b"
     * are both three characters and both sanitise to "a_b".
     */
    private static String fingerprint(String... parts) {
        StringBuilder raw = new StringBuilder();
        for (String part : parts) {
            String value = part == null ? "" : part;
            raw.append(value.length()).append(':').append(value);
        }

        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(raw.toString().getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest, 0, 4);
        } catch (NoSuchAlgorithmException e) {
            // SHA-256 is required of every JVM, so this cannot happen.
            throw new IllegalStateException(e);
        }
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
            description = """
                Identifier of the dbt Cloud run that was read.
                """
        )
        private Long runId;

        @Schema(
            title = "Lineage emitted",
            description = """
                True when this execution emitted the run's asset lineage, false when it had already been
                emitted for this run.
                """
        )
        private boolean lineageEmitted;

        @Schema(
            title = "Assets",
            description = """
                Asset ids described by the run's manifest, whether or not lineage was emitted.
                """
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
