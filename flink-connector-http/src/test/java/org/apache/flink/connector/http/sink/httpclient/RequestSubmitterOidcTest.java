/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.http.sink.httpclient;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.http.WireMockServerPortAllocator;
import org.apache.flink.connector.http.config.HttpConnectorConfigConstants;
import org.apache.flink.connector.http.config.HttpSinkConfig;
import org.apache.flink.connector.http.sink.HttpSinkRequestEntry;
import org.apache.flink.connector.http.table.sink.Slf4jHttpPostRequestCallback;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.stubbing.Scenario;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;

import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.apache.flink.connector.http.table.sink.HttpDynamicSinkConnectorOptions.SINK_OIDC_AUTH_TOKEN_ENDPOINT_URL;
import static org.apache.flink.connector.http.table.sink.HttpDynamicSinkConnectorOptions.SINK_OIDC_AUTH_TOKEN_EXPIRY_REDUCTION;
import static org.apache.flink.connector.http.table.sink.HttpDynamicSinkConnectorOptions.SINK_OIDC_AUTH_TOKEN_REQUEST;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/** Tests that both sink request submitters authorize every request with a current OIDC token. */
class RequestSubmitterOidcTest {

    private static final String TOKEN_PATH = "/token";

    private static final String TOKEN_REQUEST = "grant_type=client_credentials&client_id=sink";

    private WireMockServer wireMockServer;

    private HttpClient sinkHttpClient;

    @BeforeEach
    void setUp() {
        wireMockServer =
                new WireMockServer(
                        wireMockConfig().port(WireMockServerPortAllocator.getServerPort()));
        wireMockServer.start();

        sinkHttpClient = mock(HttpClient.class);
        HttpResponse<String> httpResponse = mock(HttpResponse.class);
        doReturn(CompletableFuture.completedFuture(httpResponse))
                .when(sinkHttpClient)
                .sendAsync(any(), any());
    }

    @AfterEach
    void tearDown() {
        wireMockServer.stop();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void shouldAddBearerTokenAndReuseItWhileValid(boolean batchMode) {
        stubToken("token-1", 3600);
        AbstractRequestSubmitter submitter = submitter(batchMode, new String[0]);

        submitter.submit("http://sink", List.of(entry())).forEach(CompletableFuture::join);
        submitter.submit("http://sink", List.of(entry())).forEach(CompletableFuture::join);

        assertThat(authorizationHeaders()).containsExactly("Bearer token-1", "Bearer token-1");
        wireMockServer.verify(1, postRequestedFor(urlEqualTo(TOKEN_PATH)));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void shouldRefreshExpiredTokenBeforeNextRequest(boolean batchMode) {
        wireMockServer.stubFor(
                post(urlEqualTo(TOKEN_PATH))
                        .inScenario("refresh")
                        .whenScenarioStateIs(Scenario.STARTED)
                        .willReturn(tokenResponse("token-1", 0))
                        .willSetStateTo("expired"));
        wireMockServer.stubFor(
                post(urlEqualTo(TOKEN_PATH))
                        .inScenario("refresh")
                        .whenScenarioStateIs("expired")
                        .willReturn(tokenResponse("token-2", 3600)));
        AbstractRequestSubmitter submitter = submitter(batchMode, new String[0]);

        submitter.submit("http://sink", List.of(entry())).forEach(CompletableFuture::join);
        submitter.submit("http://sink", List.of(entry())).forEach(CompletableFuture::join);

        assertThat(authorizationHeaders()).containsExactly("Bearer token-1", "Bearer token-2");
        wireMockServer.verify(2, postRequestedFor(urlEqualTo(TOKEN_PATH)));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void shouldReplaceConfiguredAuthorizationHeaderAndKeepOthers(boolean batchMode) {
        stubToken("token-1", 3600);
        AbstractRequestSubmitter submitter =
                submitter(
                        batchMode,
                        new String[] {
                            "authorization", "Basic static", "Content-Type", "application/json"
                        });

        submitter.submit("http://sink", List.of(entry())).forEach(CompletableFuture::join);

        HttpRequest request = sentRequests().get(0);
        assertThat(request.headers().allValues("Authorization")).containsExactly("Bearer token-1");
        assertThat(request.headers().firstValue("Content-Type")).contains("application/json");
    }

    private void stubToken(String token, int expiresInSeconds) {
        wireMockServer.stubFor(
                post(urlEqualTo(TOKEN_PATH)).willReturn(tokenResponse(token, expiresInSeconds)));
    }

    private static com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder tokenResponse(
            String token, int expiresInSeconds) {
        return aResponse()
                .withStatus(200)
                .withBody(
                        "{\"access_token\": \""
                                + token
                                + "\", \"expires_in\": "
                                + expiresInSeconds
                                + "}");
    }

    private AbstractRequestSubmitter submitter(boolean batchMode, String[] headersAndValues) {
        Configuration configuration = new Configuration();
        configuration.set(SINK_OIDC_AUTH_TOKEN_ENDPOINT_URL, wireMockServer.baseUrl() + TOKEN_PATH);
        configuration.set(SINK_OIDC_AUTH_TOKEN_REQUEST, TOKEN_REQUEST);
        configuration.set(SINK_OIDC_AUTH_TOKEN_EXPIRY_REDUCTION, Duration.ZERO);

        Properties properties = new Properties();
        properties.setProperty(HttpConnectorConfigConstants.SINK_HTTP_BATCH_REQUEST_SIZE, "10");
        HttpSinkConfig sinkConfig =
                HttpSinkConfig.builder()
                        .url("http://sink")
                        .properties(properties)
                        .readableConfig(configuration)
                        .httpPostRequestCallback(new Slf4jHttpPostRequestCallback())
                        .build();

        return batchMode
                ? new BatchRequestSubmitter(
                        sinkConfig,
                        headersAndValues,
                        sinkHttpClient,
                        Executors.newSingleThreadExecutor())
                : new PerRequestSubmitter(
                        sinkConfig,
                        headersAndValues,
                        sinkHttpClient,
                        Executors.newSingleThreadExecutor());
    }

    private static HttpSinkRequestEntry entry() {
        return new HttpSinkRequestEntry("POST", "{}".getBytes());
    }

    private List<HttpRequest> sentRequests() {
        ArgumentCaptor<HttpRequest> requestCaptor = ArgumentCaptor.forClass(HttpRequest.class);
        verify(sinkHttpClient, atLeastOnce()).sendAsync(requestCaptor.capture(), any());
        return requestCaptor.getAllValues();
    }

    private List<String> authorizationHeaders() {
        return sentRequests().stream()
                .map(request -> request.headers().firstValue("Authorization").orElse(null))
                .collect(Collectors.toList());
    }
}
