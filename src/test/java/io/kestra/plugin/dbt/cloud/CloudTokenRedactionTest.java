package io.kestra.plugin.dbt.cloud;

import org.junit.jupiter.api.Test;

import io.kestra.core.models.property.Property;
import io.kestra.core.utils.IdUtils;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;

/**
 * `token` is declared on AbstractDbtCloud, which carries a class-level @ToString. Today the concrete tasks
 * declare their own @ToString and Lombok defaults to callSuper = false, so inherited fields are not printed
 * and the token does not leak. That safety is incidental: a subclass without @ToString, or callSuper = true,
 * would start printing a credential. @ToString.Exclude makes it hold by construction, and these pin it.
 */
class CloudTokenRedactionTest {
    private static final String SECRET = "dbtc_a-token-that-must-never-be-printed";

    @Test
    void checkStatusToStringDoesNotCarryTheToken() {
        var task = CheckStatus.builder()
            .id(IdUtils.create())
            .type(CheckStatus.class.getName())
            .accountId(Property.ofValue("123"))
            .token(Property.ofValue(SECRET))
            .runId(Property.ofValue("456"))
            .build();

        assertThat(task.toString(), not(containsString(SECRET)));
    }

    @Test
    void triggerRunToStringDoesNotCarryTheToken() {
        var task = TriggerRun.builder()
            .id(IdUtils.create())
            .type(TriggerRun.class.getName())
            .accountId(Property.ofValue("123"))
            .token(Property.ofValue(SECRET))
            .jobId(Property.ofValue("789"))
            .build();

        assertThat(task.toString(), not(containsString(SECRET)));
    }
}
