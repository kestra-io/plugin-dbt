package io.kestra.plugin.dbt.cloud;

import java.io.IOException;
import java.time.Duration;
import java.util.Optional;

import org.slf4j.Logger;

import io.kestra.core.exceptions.ResourceExpiredException;
import io.kestra.core.runners.RunContext;
import io.kestra.core.storages.kv.KVMetadata;
import io.kestra.core.storages.kv.KVStore;
import io.kestra.core.storages.kv.KVValueAndMetadata;

/**
 * Remembers, recalls and forgets the dbt Cloud run id a {@link TriggerRun} triggered, so a
 * resubmitted attempt after a worker restart resumes that exact run instead of triggering a
 * duplicate. Stored in the flow namespace KV store, keyed by the taskrun id (stable across a
 * restart, unique per execution). Writes are best-effort.
 */
final class ResumeStore {
    private static final String KEY_PREFIX = "dbt_cloud_resume_";

    private final KVStore kv;
    private final String key;
    private final Duration ttl;
    private final Logger logger;

    private ResumeStore(KVStore kv, String key, Duration ttl, Logger logger) {
        this.kv = kv;
        this.key = key;
        this.ttl = ttl;
        this.logger = logger;
    }

    static ResumeStore of(RunContext runContext, Duration ttl) {
        return new ResumeStore(
            runContext.namespaceKv(runContext.flowInfo().namespace()),
            KEY_PREFIX + runContext.taskRunInfo().taskRunId(),
            ttl,
            runContext.logger()
        );
    }

    /**
     * The remembered run id, empty when nothing usable is stored (absent, expired, or corrupt). A
     * genuine read failure propagates so the task fails loudly rather than silently re-triggering.
     */
    Optional<Long> recall() throws IOException {
        try {
            return kv.getValue(key).map(value -> Long.valueOf(String.valueOf(value.value())));
        } catch (ResourceExpiredException | NumberFormatException e) {
            logger.warn("No usable remembered dbt Cloud run id, will trigger a fresh run: {}", e.getMessage());
            return Optional.empty();
        }
    }

    /** Remembers the run id so a later attempt can resume it. Best-effort. */
    void remember(Long runId) {
        try {
            kv.put(key, new KVValueAndMetadata(new KVMetadata("dbt Cloud run id kept for resume after a worker restart", ttl), runId.toString()));
        } catch (Exception e) {
            logger.warn("Could not remember dbt Cloud run {} for resume; a worker restart may trigger a duplicate", runId, e);
        }
    }

    /** Forgets the run id once it has reached a terminal state. Best-effort. */
    void forget() {
        try {
            kv.delete(key);
        } catch (Exception e) {
            logger.debug("Could not forget dbt Cloud run id", e);
        }
    }
}
