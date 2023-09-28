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

import static io.kestra.core.utils.Rethrow.throwSupplier;
import static java.lang.Math.max;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
        title = "Check job status of a running job"
)
@Plugin(
        examples = {
                @Example(
                        code = {
                                "accountId: \"<your-account>\"",
                                "token: \"<your token>\"",
                                "runId: \"<your run id>\"",
                        }
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
            title = "The run id to check status for"
    )
    @PluginProperty(dynamic = true)
    Integer runId;


    @Schema(
            title = "Specify frequency at which the job status will be checked"
    )
    @PluginProperty(dynamic = false)
    @Builder.Default
    Duration pollFrequency = Duration.ofSeconds(5);

    @Schema(
            title = "The max total wait duration"
    )
    @PluginProperty(dynamic = false)
    @Builder.Default
    Duration maxDuration = Duration.ofMinutes(60);

    @Builder.Default
    @Schema(
            title = "Parse run result",
            description = "Parsing run result to display duration of each task inside dbt"
    )
    @PluginProperty
    protected Boolean parseRunResults = true;

    @Builder.Default
    @Getter(AccessLevel.NONE)
    private transient List<JobStatusHumanizedEnum> loggedStatus = new ArrayList<>();

    @Builder.Default
    @Getter(AccessLevel.NONE)
    private transient Map<Integer, Integer> loggedSteps = new HashMap<>();

    @Override
    public CheckStatus.Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();

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
                this.pollFrequency,
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

        if (this.parseRunResults) {
            ResultParser.parseRunResult(runContext, runResultsArtifact.toFile());
        }

        return Output.builder()
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
                    for (String s : step.getLogs().substring(max(loggedSteps.get(step.getId()) -1, 0)).split("\n")) {
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
                title = "URI of a run results"
        )
        private URI runResults;

        @Schema(
                title = "URI of a manifest"
        )
        private URI manifest;
    }
}
