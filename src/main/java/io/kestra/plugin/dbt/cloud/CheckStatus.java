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

import org.slf4j.Logger;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.http.HttpRequest;
import io.kestra.core.http.client.HttpClientException;
import io.kestra.core.http.client.HttpClientResponseException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.utils.Await;
import io.kestra.plugin.dbt.ResultParser;
import io.kestra.plugin.dbt.cloud.models.JobStatus;
import io.kestra.plugin.dbt.cloud.models.JobStatusHumanizedEnum;
import io.kestra.plugin.dbt.cloud.models.ManifestArtifact;
import io.kestra.plugin.dbt.cloud.models.Run;
import io.kestra.plugin.dbt.cloud.models.RunResponse;
import io.kestra.plugin.dbt.cloud.models.Step;
import io.kestra.plugin.dbt.models.RunResult;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

import static io.kestra.core.utils.Rethrow.throwSupplier;
import static java.lang.Math.max;
import io.kestra.core.models.annotations.PluginProperty;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Monitor a dbt Cloud run",
    description = "Polls a dbt Cloud run until it ends, streaming step logs and downloading artifacts. Fails on non-successful statuses; defaults to 5s polling and a 60m timeout, and can parse run results for node timings."
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
                    accountId: "dbt_account"
                    token: "{{ secret('DBT_TOKEN') }}"
                    runId: "run_id"
                """
        )
    }
)
public class CheckStatus extends AbstractDbtCloud implements RunnableTask<CheckStatus.Output> {
    private static final Set<JobStatus> ENDED_STATUS = Set.of(
        JobStatus.NUMBER_10,  // Success
        JobStatus.NUMBER_20,  // Error
        JobStatus.NUMBER_30   // Cancelled
    );

    @Schema(
        title = "Run ID",
        description = "dbt Cloud run identifier to monitor."
    )
    @NotNull
    Property<String> runId;

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
    @Getter(AccessLevel.NONE)
    private transient List<JobStatusHumanizedEnum> loggedStatus = new ArrayList<>();

    @Builder.Default
    @Getter(AccessLevel.NONE)
    private transient Map<Long, Long> loggedSteps = new HashMap<>();

    @Override
    public CheckStatus.Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();

        // Check rendered runId provided is an Integer
        Long runIdRendered = Long.parseLong(runContext.render(this.runId).as(String.class).orElseThrow());

        // wait for end
        RunResponse finalRunResponse = Await.until(
            throwSupplier(() ->
            {
                Optional<RunResponse> fetchRunResponse = fetchRunResponse(
                    runContext,
                    runIdRendered,
                    false
                );

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

        if (!isSuccessful(finalRunResponse.getData())) {
            throw new Exception(
                "Failed run with status '" + finalRunResponse.getData().getStatusHumanized() +
                    "' after " + finalRunResponse.getData().getDurationHumanized() +
                    (finalRunResponse.getData().getStatusMessage() != null
                        ? ": " + finalRunResponse.getData().getStatusMessage()
                        : "") +
                    ": " + finalRunResponse
            );
        }

        // Artifacts are uploaded asynchronously by dbt Cloud and manifest.json is absent for some
        // run shapes (e.g. dbt source freshness). Tolerate 404 so a legitimate success is not
        // reported as a failure.
        Path runResultsArtifact = downloadArtifacts(runContext, runIdRendered, "run_results.json", RunResult.class);
        Path manifestArtifact = downloadArtifacts(runContext, runIdRendered, "manifest.json", ManifestArtifact.class);

        io.kestra.plugin.dbt.models.Manifest manifest = null;
        URI manifestUri = null;
        if (manifestArtifact != null) {
            ResultParser.ManifestResult manifestResult = ResultParser.parseManifestWithAssets(runContext, manifestArtifact.toFile());
            manifest = manifestResult.manifest();
            manifestUri = manifestResult.uri();
        }

        URI runResultsUri = null;

        if (runResultsArtifact != null) {
            if (runContext.render(this.parseRunResults).as(Boolean.class).orElse(false)) {
                runResultsUri = ResultParser.parseRunResult(runContext, runResultsArtifact.toFile(), manifest);
            } else {
                runResultsUri = runContext.storage().putFile(runResultsArtifact.toFile());
            }
        }

        return Output.builder()
            .runResults(runResultsUri)
            .manifest(manifestUri)
            .build();
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
                        "/?include_related=" + URLEncoder.encode("[\"trigger\",\"job\"," + (debug ? "\"debug_logs\"" : "") + ",\"run_steps\", \"environment\"]", StandardCharsets.UTF_8)
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
