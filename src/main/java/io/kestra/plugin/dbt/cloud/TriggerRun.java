package io.kestra.plugin.dbt.cloud;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.slf4j.Logger;

import io.kestra.core.exceptions.ResourceExpiredException;
import io.kestra.core.http.HttpRequest;
import io.kestra.core.http.HttpResponse;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.storages.kv.KVMetadata;
import io.kestra.core.storages.kv.KVStore;
import io.kestra.core.storages.kv.KVValueAndMetadata;
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
        title = "Resume on worker restart",
        description = "If true (default), remembers the dbt Cloud run ID after triggering and, on a worker restart, resumes the in-flight run instead of triggering a duplicate. Only applies when wait is true."
    )
    @Builder.Default
    @PluginProperty(group = "reliability")
    Property<Boolean> resumeOnRestart = Property.ofValue(Boolean.TRUE);

    @Schema(
        title = "Poll frequency",
        description = "Interval between status checks when waiting. Default 5s."
    )
    @Builder.Default
    Property<Duration> pollFrequency = Property.ofValue(Duration.ofSeconds(5));

    private static final Duration DEFAULT_MAX_DURATION = Duration.ofMinutes(60);

    @Schema(
        title = "Max wait duration",
        description = "Ceiling for waiting on job completion. Default 60m."
    )
    @Builder.Default
    Property<Duration> maxDuration = Property.ofValue(DEFAULT_MAX_DURATION);

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

        boolean waiting = !Boolean.FALSE.equals(runContext.render(this.wait).as(Boolean.class).orElse(Boolean.TRUE));
        boolean resumeEnabled = waiting && Boolean.TRUE.equals(runContext.render(this.resumeOnRestart).as(Boolean.class).orElse(Boolean.TRUE));

        KVStore kv = null;
        String resumeKey = null;
        Long runId = null;

        if (resumeEnabled) {
            Duration ttl = runContext.render(this.maxDuration).as(Duration.class).orElse(DEFAULT_MAX_DURATION).multipliedBy(2);
            kv = runContext.namespaceKv(runContext.flowInfo().namespace());
            resumeKey = "dbt_cloud_resume_" + runContext.taskRunInfo().taskRunId();

            Optional<Long> rememberedRunId = recallRunId(logger, kv, resumeKey);
            if (rememberedRunId.isPresent()) {
                runId = rememberedRunId.get();
                logger.info("Resuming in-flight dbt Cloud run {}", runId);
                rememberRunId(logger, kv, resumeKey, runId, ttl); // refresh TTL
            } else {
                runId = triggerRun(runContext, logger);
                rememberRunId(logger, kv, resumeKey, runId, ttl);
            }
        } else {
            runId = triggerRun(runContext, logger);
        }

        if (!waiting) {
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

        CheckStatus.Output runOutput;
        try {
            runOutput = checkStatusJob.run(runContext);
        } catch (CheckStatus.RunFailedException e) {
            // Confirmed failure: forget so a retry triggers a fresh run.
            if (resumeEnabled) {
                forgetRunId(logger, kv, resumeKey);
            }
            throw e;
        }
        // Any other exception (timeout, or unconfirmed status like 401/403/404) keeps the run id: it
        // may still be live, so a restart resumes it rather than duplicating.

        if (resumeEnabled) {
            forgetRunId(logger, kv, resumeKey);
        }

        return Output.builder()
            .runId(runId)
            .runResults(runOutput.getRunResults())
            .manifest(runOutput.getManifest())
            .build();
    }

    private Long triggerRun(RunContext runContext, Logger logger) throws Exception {
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
        return triggerRunResponse.getData().getId();
    }

    private static Optional<Long> recallRunId(Logger logger, KVStore kv, String resumeKey) throws IOException {
        try {
            return kv.getValue(resumeKey).map(value -> Long.valueOf(String.valueOf(value.value())));
        } catch (ResourceExpiredException | NumberFormatException e) {
            // Expired or a corrupt/non-numeric value: nothing usable to resume, so trigger fresh.
            logger.warn("No usable remembered dbt Cloud run id, will trigger a fresh run: {}", e.getMessage());
            return Optional.empty();
        }
        // A KVStore read failure (IOException) is left to propagate: the run may be remembered but
        // unreadable, so fail loudly rather than silently trigger a duplicate.
    }

    private static void rememberRunId(Logger logger, KVStore kv, String resumeKey, Long runId, Duration ttl) {
        try {
            kv.put(resumeKey, new KVValueAndMetadata(new KVMetadata("dbt Cloud run id, kept so the run can resume after a worker restart", ttl), runId.toString()));
        } catch (Exception e) {
            logger.warn("Could not remember dbt Cloud run {} for resume; a worker restart during this run may trigger a duplicate", runId, e);
        }
    }

    private static void forgetRunId(Logger logger, KVStore kv, String resumeKey) {
        try {
            kv.delete(resumeKey);
        } catch (Exception e) {
            logger.debug("Could not forget dbt Cloud run id '{}': {}", resumeKey, e.getMessage());
        }
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
