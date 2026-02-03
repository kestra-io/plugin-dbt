package io.kestra.plugin.dbt.cloud;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.http.HttpRequest;
import io.kestra.core.http.client.HttpClientException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.utils.Await;
import io.kestra.plugin.dbt.ResultParser;
import io.kestra.plugin.dbt.cloud.models.ManifestArtifact;
import io.kestra.plugin.dbt.cloud.models.JobStatusHumanizedEnum;
import io.kestra.plugin.dbt.cloud.models.RunResponse;
import io.kestra.plugin.dbt.cloud.models.Step;

import io.kestra.plugin.dbt.models.RunResult;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.*;

import static io.kestra.core.utils.Rethrow.throwSupplier;
import static java.lang.Math.max;

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
    private static final List<JobStatusHumanizedEnum> ENDED_STATUS = List.of(
            JobStatusHumanizedEnum.ERROR,
            JobStatusHumanizedEnum.CANCELLED,
            JobStatusHumanizedEnum.SUCCESS
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
                throwSupplier(() -> {
                    Optional<RunResponse> fetchRunResponse = fetchRunResponse(
                            runContext,
                            runIdRendered,
                            false
                    );

                    if (fetchRunResponse.isPresent()) {
                        logSteps(logger, fetchRunResponse.get());

                        // we rely on truncated logs to be sure
                        boolean allLogs = fetchRunResponse.get()
                                .getData()
                                .getRunSteps()
                                .stream()
                                .filter(step -> step.getTruncatedDebugLogs() != null)
                                .count() ==
                                fetchRunResponse.get()
                                        .getData()
                                        .getRunSteps().size();

                        // ended
                        if (ENDED_STATUS.contains(fetchRunResponse.get().getData().getStatusHumanized()) && allLogs) {
                            return fetchRunResponse.get();
                        }
                    }

                    return null;
                }),
                runContext.render(this.pollFrequency).as(Duration.class).orElseThrow(),
                runContext.render(this.maxDuration).as(Duration.class).orElseThrow()
        );

        // final response
        logSteps(logger, finalRunResponse);

        if (!finalRunResponse.getData().getStatusHumanized().equals(JobStatusHumanizedEnum.SUCCESS)) {
            throw new Exception("Failed run with status '" + finalRunResponse.getData().getStatusHumanized() +
                    "' after " +  finalRunResponse.getData().getDurationHumanized() + ": " + finalRunResponse
            );
        }

        Path runResultsArtifact = downloadArtifacts(runContext, runIdRendered, "run_results.json", RunResult.class);
        Path manifestArtifact = downloadArtifacts(runContext, runIdRendered, "manifest.json", ManifestArtifact.class);


        URI runResultsUri = null;

        if (Boolean.TRUE.equals(runContext.render(this.parseRunResults).as(Boolean.class).orElse(false))) {
            runResultsUri = ResultParser.parseRunResult(runContext, runResultsArtifact.toFile());
        } else {
            if (Files.exists(runResultsArtifact)) {
                runResultsUri = runContext.storage().putFile(runResultsArtifact.toFile());
            }
        }

        return Output.builder()
                .runResults(runResultsUri)
                .manifest(manifestArtifact.toFile().exists() ? runContext.storage().putFile(manifestArtifact.toFile()) : null)
                .build();
    }

    private void logSteps(Logger logger, RunResponse runResponse) {
        // status changed
        if (!loggedStatus.contains(runResponse.getData().getStatusHumanized())) {
            logger.debug("Status changed to '{}' after {}",
                    runResponse.getData().getStatusHumanized(),
                    runResponse.getData().getDurationHumanized()
            );
            loggedStatus.add(runResponse.getData().getStatusHumanized());
        }

        // log steps
        for (Step step : runResponse.getData().getRunSteps()) {
            if (!step.getLogs().isEmpty()){
                if (!loggedSteps.containsKey(step.getId())){
                    loggedSteps.put(step.getId(), 0L);
                }

                if (step.getLogs().length() > loggedSteps.get(step.getId())) {
                    for (String s : step.getLogs().substring((int) max(loggedSteps.get(step.getId()) -1L, 0L)).split("\n")) {
                        logger.info("[Step {}]: {}", step.getName(), s);
                    }
                    loggedSteps.put(step.getId(), (long) step.getLogs().length());
                }
            }
        }
    }

    private Optional<RunResponse> fetchRunResponse(RunContext runContext, Long id, Boolean debug) throws IllegalVariableEvaluationException, HttpClientException, IOException {
        HttpRequest.HttpRequestBuilder requestBuilder = HttpRequest.builder()
            .uri(URI.create(runContext.render(this.baseUrl).as(String.class).orElseThrow() + "/api/v2/accounts/" + runContext.render(this.accountId).as(String.class).orElseThrow() + "/runs/" + id +
                "/?include_related=" + URLEncoder.encode("[\"trigger\",\"job\"," + (debug ? "\"debug_logs\"" : "") + ",\"run_steps\", \"environment\"]", StandardCharsets.UTF_8)))
            .method("GET");

        return Optional.ofNullable(this.request(runContext, requestBuilder, RunResponse.class).getBody());
    }

    private <T> Path downloadArtifacts(RunContext runContext, Long runId, String path, Class<T> responseType)
        throws IllegalVariableEvaluationException, IOException, HttpClientException {
        HttpRequest.HttpRequestBuilder requestBuilder = HttpRequest.builder()
            .uri(URI.create(runContext.render(this.baseUrl).as(String.class).orElseThrow()
                + "/api/v2/accounts/" + runContext.render(this.accountId).as(String.class).orElseThrow()
                + "/runs/" + runId + "/artifacts/" + path))
            .method("GET");

        T artifact = this.request(runContext, requestBuilder, responseType).getBody();

        String artifactJson = JacksonMapper.ofJson().writeValueAsString(artifact);

        Path tempFile = runContext.workingDir().createTempFile(".json");
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
