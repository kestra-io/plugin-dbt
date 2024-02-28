package io.kestra.plugin.dbt.cloud;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.micronaut.core.type.Argument;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.MediaType;
import io.micronaut.http.MutableHttpRequest;
import io.micronaut.http.client.DefaultHttpClientConfiguration;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import io.micronaut.http.client.netty.DefaultHttpClient;
import io.micronaut.http.client.netty.NettyHttpClientFactory;
import io.micronaut.http.codec.MediaTypeCodecRegistry;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import jakarta.validation.constraints.NotNull;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractDbtCloud extends Task {
    @Schema(
        title = "Base url to select the tenant."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    @Builder.Default
    String baseUrl = "https://cloud.getdbt.com";

    @Schema(
        title = "Numeric ID of the account."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    String accountId;

    @Schema(
        title = "API key."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    String token;

    private static final Duration HTTP_READ_TIMEOUT = Duration.ofSeconds(60);
    private static final NettyHttpClientFactory FACTORY = new NettyHttpClientFactory();

    protected HttpClient client(RunContext runContext) throws IllegalVariableEvaluationException, MalformedURLException, URISyntaxException {
        MediaTypeCodecRegistry mediaTypeCodecRegistry = runContext.getApplicationContext().getBean(MediaTypeCodecRegistry.class);

        var httpConfig = new DefaultHttpClientConfiguration();
        httpConfig.setMaxContentLength(Integer.MAX_VALUE);
        httpConfig.setReadTimeout(HTTP_READ_TIMEOUT);

        DefaultHttpClient client = (DefaultHttpClient) FACTORY.createClient(URI.create(runContext.render(baseUrl)).toURL(), httpConfig);
        client.setMediaTypeCodecRegistry(mediaTypeCodecRegistry);

        return client;
    }

    protected <REQ, RES> HttpResponse<RES> request(RunContext runContext,
                                                   MutableHttpRequest<REQ> request,
                                                   Argument<RES> argument) throws HttpClientResponseException {
        return request(runContext, request, argument, null);
    }
    protected <REQ, RES> HttpResponse<RES> request(RunContext runContext,
                                                   MutableHttpRequest<REQ> request,
                                                   Argument<RES> argument,
                                                   Duration timeout) throws HttpClientResponseException {
        try {
            request = request
                .bearerAuth(runContext.render(this.token))
                .contentType(MediaType.APPLICATION_JSON);

            try (HttpClient client = this.client(runContext)) {
                Mono<HttpResponse<RES>> mono = Mono.from(client.exchange(request, argument));
                return timeout != null ? mono.block(timeout) : mono.block();
            }
        } catch (HttpClientResponseException e) {
            throw new HttpClientResponseException(
                "Request failed '" + e.getStatus().getCode() + "' and body '" + e.getResponse().getBody(String.class).orElse("null") + "'",
                e,
                e.getResponse()
            );
        } catch (IllegalVariableEvaluationException | MalformedURLException | URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }
}
