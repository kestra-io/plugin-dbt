package io.kestra.plugin.dbt.cloud;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.utils.Await;
import io.kestra.plugin.dbt.ResultParser;
import io.kestra.plugin.dbt.cloud.models.JobStatusHumanizedEnum;
import io.kestra.plugin.dbt.cloud.models.RunResponse;
import io.kestra.plugin.dbt.cloud.models.Step;
import io.micronaut.core.type.Argument;
import io.micronaut.http.HttpMethod;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.uri.UriTemplate;
import io.swagger.v3.oas.annotations.media.Schema;
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
import javax.validation.constraints.NotNull;

import static io.kestra.core.utils.Rethrow.throwSupplier;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Trigger job to run",
    description = "Use this task to kick off a run for a job. When this endpoint returns a successful response, a " +
        "new run will be enqueued for the account. If you activate the `wait` option, it will wait for the job to be ended " +
        "and will display all the log and dynamic tasks."
)
@Plugin(
    examples = {
        @Example(
            code = {
                "accountId: \"<your-account>\"",
                "token: \"<your token>\"",
                "jobId: \"<your job id>\"",
            }
        )
    }
)
public class TriggerRun extends AbstractDbtCloud implements RunnableTask<TriggerRun.Output> {
    private static final List<JobStatusHumanizedEnum> ENDED_STATUS = List.of(
        JobStatusHumanizedEnum.ERROR,
        JobStatusHumanizedEnum.CANCELLED,
        JobStatusHumanizedEnum.SUCCESS
    );

    @Schema(
        title = "Numeric ID of the job"
    )
    @PluginProperty(dynamic = true)
    @NotNull
    String jobId;

    @Schema(
        title = "A text description of the reason for running this job"
    )
    @PluginProperty(dynamic = true)
    @Builder.Default
    @NotNull
    String cause = "Triggered by Kestra";

    @Schema(
        title = "The git sha to check out before running this job"
    )
    @PluginProperty(dynamic = true)
    String gitSha;

    @Schema(
        title = "The git branch to check out before running this job"
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
        description = "Allowing to capture job status & logs"
    )
    @PluginProperty(dynamic = false)
    @Builder.Default
    Boolean wait = true;

    @Schema(
        title = "The max total wait duration"
    )
    @PluginProperty(dynamic = false)
    @Builder.Default
    Duration maxDuration = Duration.ofMinutes(60);

    @Builder.Default
    @Getter(AccessLevel.NONE)
    private transient List<JobStatusHumanizedEnum> loggedStatus = new ArrayList<>();

    @Builder.Default
    @Getter(AccessLevel.NONE)
    private transient Map<Integer, Integer> loggedSteps = new HashMap<>();

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
        Integer runId = triggerRunResponse.getData().getId();

        if (!this.wait) {
            return Output.builder()
                .runId(runId)
                .build();
        }

        // wait for end
        RunResponse finalRunResponse = Await.until(
            throwSupplier(() -> {
                Optional<RunResponse> fetchRunResponse = fetchRunResponse(
                    runContext,
                    runId,
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
            Duration.ofSeconds(1),
            this.maxDuration
        );

        // final response
        logSteps(logger, finalRunResponse);

        if (!finalRunResponse.getData().getStatusHumanized().equals(JobStatusHumanizedEnum.SUCCESS)) {
            throw new Exception("Failed run with status '" + finalRunResponse.getData().getStatusHumanized() +
                "' after " +  finalRunResponse.getData().getDurationHumanized() + ": " + finalRunResponse
            );
        }

        Path runResultsArtifact = downloadArtifacts(runContext, runId, "run_results.json");
        Path manifestArtifact = downloadArtifacts(runContext, runId, "manifest.json");
        ResultParser.parseRunResult(runContext, runResultsArtifact.toFile());

        return Output.builder()
            .runId(runId)
            .runResults(runResultsArtifact.toFile().exists() ? runContext.putTempFile(runResultsArtifact.toFile()) : null)
            .manifest(manifestArtifact.toFile().exists() ? runContext.putTempFile(manifestArtifact.toFile()) : null)
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
                    loggedSteps.put(step.getId(), 0);
                }

                if (step.getLogs().length() > loggedSteps.get(step.getId())) {
                    for (String s : step.getLogs().substring(loggedSteps.get(step.getId()) -1).split("\n")) {
                        logger.info("[Step {}]: {}", step.getName(), s);
                    }
                    loggedSteps.put(step.getId(), step.getLogs().length());
                }
            }
        }
    }

    private Optional<RunResponse> fetchRunResponse(RunContext runContext, Integer id, Boolean debug) throws IllegalVariableEvaluationException {
        return this
            .request(
                runContext,
                HttpRequest
                    .create(
                        HttpMethod.GET,
                        UriTemplate
                            .of("/api/v2/accounts/{accountId}/runs/{runId}/" +
                                    "?include_related=" + URLEncoder.encode(
                                    "[\"trigger\",\"job\"," + (debug ? "\"debug_logs\"" : "") + ",\"run_steps\", \"environment\"]",
                                    StandardCharsets.UTF_8
                                )
                            )
                            .expand(Map.of(
                                "accountId", runContext.render(this.accountId),
                                "runId", id
                            ))
                    ),
                Argument.of(RunResponse.class)
            )
            .getBody();
    }

    private Path downloadArtifacts(RunContext runContext, Integer runId, String path) throws IllegalVariableEvaluationException, IOException {
        String artifact = this
            .request(
                runContext,
                HttpRequest
                    .create(
                        HttpMethod.GET,
                        UriTemplate
                            .of("/api/v2/accounts/{accountId}/runs/{runId}/artifacts/{path}")
                            .expand(Map.of(
                                "accountId", runContext.render(this.accountId),
                                "runId", runId,
                                "path", path
                            ))
                    ),
                Argument.of(String.class)
            )
            .getBody()
            .orElseThrow();

        Path tempFile = runContext.tempFile(".json");

        Files.writeString(tempFile, artifact, StandardOpenOption.TRUNCATE_EXISTING);

        return tempFile;
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "The run id."
        )
        private Integer runId;

        @Schema(
            title = "URI of a run results"
        )
        private URI runResults;

        @Schema(
            title = "URI of a manifest"
        )
        private URI manifest;
    }
}
