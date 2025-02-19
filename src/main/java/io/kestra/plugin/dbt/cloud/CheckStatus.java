package io.kestra.plugin.dbt.cloud;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.utils.Await;
import io.kestra.plugin.dbt.ResultParser;
import io.kestra.plugin.dbt.cloud.models.JobStatusHumanizedEnum;
import io.kestra.plugin.dbt.cloud.models.RunResponse;
import io.kestra.plugin.dbt.cloud.models.Step;

import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
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
    title = "Check the status of a dbt Cloud job."
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
                    token: "dbt_token"
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
            title = "The job run ID to check the status for."
    )
    @NotNull
    Property<String> runId;


    @Schema(
            title = "Specify how often the task should poll for the job status."
    )
    @Builder.Default
    Property<Duration> pollFrequency = Property.of(Duration.ofSeconds(5));

    @Schema(
            title = "The maximum duration the task should poll for the job completion."
    )
    @Builder.Default
    Property<Duration> maxDuration = Property.of(Duration.ofMinutes(60));

    @Builder.Default
    @Schema(
            title = "Parse run result.",
            description = "Whether to parse the run result to display the duration of each dbt node in the Gantt view."
    )
    protected Property<Boolean> parseRunResults = Property.of(Boolean.TRUE);

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

        Path runResultsArtifact = downloadArtifacts(runContext, runIdRendered, "run_results.json");
        Path manifestArtifact = downloadArtifacts(runContext, runIdRendered, "manifest.json");

        if (runContext.render(this.parseRunResults).as(Boolean.class).orElseThrow()) {
            ResultParser.parseRunResult(runContext, runResultsArtifact.toFile());
        }

        return Output.builder()
                .runResults(runResultsArtifact.toFile().exists() ? runContext.storage().putFile(runResultsArtifact.toFile()) : null)
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

    private Optional<RunResponse> fetchRunResponse(RunContext runContext, Long id, Boolean debug) throws Exception {
        String accountId = runContext.render(this.accountId).as(String.class).orElseThrow();
        Duration maxDuration = runContext.render(this.maxDuration).as(Duration.class).orElseThrow();
        String baseUrlString = runContext.render(this.baseUrl).as(String.class).orElseThrow();

        String relatedItems = "[\"trigger\",\"job\"," + (debug ? "\"debug_logs\"" : "") + ",\"run_steps\", \"environment\"]";
        String encodedRelatedItems = URLEncoder.encode(relatedItems, StandardCharsets.UTF_8);

        String path = String.format("/api/v2/accounts/%s/runs/%d/?include_related=%s",
            accountId,
            id,
            encodedRelatedItems
        );

        URI uri = URI.create(baseUrlString + path);

        HttpRequest.Builder requestBuilder = HttpRequest.newBuilder()
            .uri(uri)
            .GET();

        RunResponse response = request(runContext, requestBuilder, RunResponse.class ,maxDuration);

        return Optional.of(response);

    }

    private Path downloadArtifacts(RunContext runContext, Long runId, String path) throws Exception {
        String accountId = runContext.render(this.accountId).as(String.class).orElseThrow();
        String baseUrlString = runContext.render(this.baseUrl).as(String.class).orElseThrow();

        String artifactPath = String.format("/api/v2/accounts/%s/runs/%d/artifacts/%s",
            accountId,
            runId,
            path
        );

        URI uri = URI.create(baseUrlString + artifactPath);

        HttpRequest.Builder requestBuilder = HttpRequest.newBuilder()
            .uri(uri)
            .GET();

        String response = request(runContext, requestBuilder, String.class, null);

        Path tempFile = runContext.workingDir().createTempFile(".json");
        Files.writeString(tempFile, response, StandardOpenOption.TRUNCATE_EXISTING);

        return tempFile;
    }
    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
                title = "URI of the run result"
        )
        private URI runResults;

        @Schema(
                title = "URI of a manifest"
        )
        private URI manifest;
    }
}
