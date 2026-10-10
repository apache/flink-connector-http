/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.http.preprocessor;

import org.apache.flink.connector.http.WireMockServerPortAllocator;
import org.apache.flink.util.InstantiationUtil;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.stubbing.Scenario;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Optional;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static org.assertj.core.api.Assertions.assertThat;

class OIDCAuthHeaderValuePreprocessorTest {

    private static final String TOKEN_ENDPOINT = "/oauth/token";

    private WireMockServer wireMockServer;
    private int serverPort;

    @BeforeEach
    public void setup() {
        serverPort = WireMockServerPortAllocator.getServerPort();
        wireMockServer =
                new WireMockServer(WireMockConfiguration.wireMockConfig().port(serverPort));
        wireMockServer.start();
    }

    @AfterEach
    public void tearDown() {
        wireMockServer.stop();
    }

    @Test
    public void shouldReturnBearerTokenWithCorrectCasing() {
        // Setup mock OIDC token endpoint
        String accessToken = "test_access_token_12345";
        wireMockServer.stubFor(
                post(urlEqualTo(TOKEN_ENDPOINT))
                        .willReturn(
                                aResponse()
                                        .withStatus(200)
                                        .withHeader("Content-Type", "application/json")
                                        .withBody(
                                                "{\"access_token\": \""
                                                        + accessToken
                                                        + "\", \"expires_in\": 3600}")));

        String tokenEndpointUrl = "http://localhost:" + serverPort + TOKEN_ENDPOINT;
        String tokenRequest = "grant_type=client_credentials&client_id=test&client_secret=secret";

        OIDCAuthHeaderValuePreprocessor preprocessor =
                new OIDCAuthHeaderValuePreprocessor(
                        tokenEndpointUrl, tokenRequest, Optional.of(Duration.ofSeconds(1)));

        String headerValue = preprocessor.preprocessHeaderValue("ignored");

        // Verify the Bearer token uses correct RFC 6750 casing ("Bearer" not "BEARER")
        assertThat(headerValue).startsWith("Bearer ");
        assertThat(headerValue).isEqualTo("Bearer " + accessToken);
        // Explicitly verify it's NOT using uppercase BEARER
        assertThat(headerValue).doesNotStartWith("BEARER ");
    }

    @Test
    public void shouldReturnBearerTokenWithDefaultExpiryReduction() {
        // Setup mock OIDC token endpoint
        String accessToken = "another_test_token";
        wireMockServer.stubFor(
                post(urlEqualTo(TOKEN_ENDPOINT))
                        .willReturn(
                                aResponse()
                                        .withStatus(200)
                                        .withHeader("Content-Type", "application/json")
                                        .withBody(
                                                "{\"access_token\": \""
                                                        + accessToken
                                                        + "\", \"expires_in\": 3600}")));

        String tokenEndpointUrl = "http://localhost:" + serverPort + TOKEN_ENDPOINT;
        String tokenRequest = "grant_type=client_credentials";

        OIDCAuthHeaderValuePreprocessor preprocessor =
                new OIDCAuthHeaderValuePreprocessor(
                        tokenEndpointUrl, tokenRequest, Optional.empty());

        String headerValue = preprocessor.preprocessHeaderValue("any_raw_value");

        // Verify correct Bearer casing per RFC 6750
        assertThat(headerValue).startsWith("Bearer ");
        assertThat(headerValue).isEqualTo("Bearer " + accessToken);
    }

    @Test
    public void shouldReuseCachedTokenWhileValid() {
        stubToken(Scenario.STARTED, "token-1", 3600, "second");
        stubToken("second", "token-2", 3600, "second");

        OIDCAuthHeaderValuePreprocessor preprocessor = preprocessor();

        assertThat(preprocessor.preprocessHeaderValue("ignored")).isEqualTo("Bearer token-1");
        assertThat(preprocessor.preprocessHeaderValue("ignored")).isEqualTo("Bearer token-1");
        wireMockServer.verify(1, postRequestedFor(urlEqualTo(TOKEN_ENDPOINT)));
    }

    @Test
    public void shouldRefreshExpiredToken() {
        stubToken(Scenario.STARTED, "token-1", 0, "second");
        stubToken("second", "token-2", 3600, "second");

        OIDCAuthHeaderValuePreprocessor preprocessor = preprocessor();

        assertThat(preprocessor.preprocessHeaderValue("ignored")).isEqualTo("Bearer token-1");
        assertThat(preprocessor.preprocessHeaderValue("ignored")).isEqualTo("Bearer token-2");
        wireMockServer.verify(2, postRequestedFor(urlEqualTo(TOKEN_ENDPOINT)));
    }

    @Test
    public void shouldWorkAfterSerialization() throws Exception {
        stubToken(Scenario.STARTED, "token-1", 3600, Scenario.STARTED);

        OIDCAuthHeaderValuePreprocessor preprocessor = preprocessor();
        preprocessor.preprocessHeaderValue("ignored");
        OIDCAuthHeaderValuePreprocessor copy =
                InstantiationUtil.clone(preprocessor, getClass().getClassLoader());

        assertThat(copy.preprocessHeaderValue("ignored")).isEqualTo("Bearer token-1");
    }

    private OIDCAuthHeaderValuePreprocessor preprocessor() {
        return new OIDCAuthHeaderValuePreprocessor(
                "http://localhost:" + serverPort + TOKEN_ENDPOINT,
                "grant_type=client_credentials",
                Optional.of(Duration.ZERO));
    }

    private void stubToken(
            String state, String accessToken, int expiresInSeconds, String nextState) {
        wireMockServer.stubFor(
                post(urlEqualTo(TOKEN_ENDPOINT))
                        .inScenario("token")
                        .whenScenarioStateIs(state)
                        .willReturn(
                                aResponse()
                                        .withStatus(200)
                                        .withHeader("Content-Type", "application/json")
                                        .withBody(
                                                "{\"access_token\": \""
                                                        + accessToken
                                                        + "\", \"expires_in\": "
                                                        + expiresInSeconds
                                                        + "}"))
                        .willSetStateTo(nextState));
    }
}
