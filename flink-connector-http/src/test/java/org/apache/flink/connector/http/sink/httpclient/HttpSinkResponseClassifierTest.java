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
import org.apache.flink.connector.http.config.HttpConnectorConfigConstants;
import org.apache.flink.connector.http.config.HttpSinkConfig;
import org.apache.flink.connector.http.table.sink.Slf4jHttpPostRequestCallback;

import org.junit.jupiter.api.Test;

import java.net.http.HttpResponse;
import java.util.Properties;

import static org.apache.flink.connector.http.table.sink.HttpDynamicSinkConnectorOptions.SINK_HTTP_IGNORED_RESPONSE_CODES;
import static org.apache.flink.connector.http.table.sink.HttpDynamicSinkConnectorOptions.SINK_HTTP_RETRY_CODES;
import static org.apache.flink.connector.http.table.sink.HttpDynamicSinkConnectorOptions.SINK_HTTP_SUCCESS_CODES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Tests for {@link HttpSinkResponseClassifier}. */
class HttpSinkResponseClassifierTest {

    @Test
    void shouldClassifyResponseCodes() {
        Configuration configuration = new Configuration();
        configuration.set(SINK_HTTP_SUCCESS_CODES, "2XX");
        configuration.set(SINK_HTTP_RETRY_CODES, "500,503,504");
        configuration.set(SINK_HTTP_IGNORED_RESPONSE_CODES, "404");

        HttpSinkResponseClassifier classifier =
                new HttpSinkResponseClassifier(config(configuration));

        assertThat(classifier.classify(response(200))).isEqualTo(HttpSinkResponseStatus.SUCCESS);
        assertThat(classifier.classify(response(404))).isEqualTo(HttpSinkResponseStatus.IGNORED);
        assertThat(classifier.classify(response(500)))
                .isEqualTo(HttpSinkResponseStatus.RETRYABLE_FAILURE);
        assertThat(classifier.classify(response(400)))
                .isEqualTo(HttpSinkResponseStatus.FATAL_FAILURE);
        assertThat(classifier.classify(null)).isEqualTo(HttpSinkResponseStatus.RETRYABLE_FAILURE);
    }

    @Test
    void shouldRejectOverlappingSuccessAndRetryCodes() {
        Configuration configuration = new Configuration();
        configuration.set(SINK_HTTP_SUCCESS_CODES, "2XX,500");
        configuration.set(SINK_HTTP_RETRY_CODES, "500");

        assertThatThrownBy(() -> new HttpSinkResponseClassifier(config(configuration)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid HTTP sink status-code configuration");
    }

    @Test
    void shouldLetIgnoredCodesOverrideRetryCodes() {
        Configuration configuration = new Configuration();
        configuration.set(SINK_HTTP_SUCCESS_CODES, "2XX");
        configuration.set(SINK_HTTP_RETRY_CODES, "500,503,504");
        configuration.set(SINK_HTTP_IGNORED_RESPONSE_CODES, "500");

        HttpSinkResponseClassifier classifier =
                new HttpSinkResponseClassifier(config(configuration));

        assertThat(classifier.classify(response(500))).isEqualTo(HttpSinkResponseStatus.IGNORED);
        assertThat(classifier.classify(response(503)))
                .isEqualTo(HttpSinkResponseStatus.RETRYABLE_FAILURE);
    }

    @Test
    void shouldClassifyLegacyErrorCodePropertyAsFatalFailure() {
        Properties properties = new Properties();
        properties.setProperty(HttpConnectorConfigConstants.HTTP_ERROR_SINK_CODES_LIST, "4XX");

        HttpSinkResponseClassifier classifier =
                new HttpSinkResponseClassifier(config(new Configuration(), properties));

        assertThat(classifier.classify(response(200))).isEqualTo(HttpSinkResponseStatus.SUCCESS);
        assertThat(classifier.classify(response(404)))
                .isEqualTo(HttpSinkResponseStatus.FATAL_FAILURE);
        assertThat(classifier.classify(response(500))).isEqualTo(HttpSinkResponseStatus.SUCCESS);
        assertThat(classifier.classify(null)).isEqualTo(HttpSinkResponseStatus.RETRYABLE_FAILURE);
    }

    @Test
    void shouldClassifyLegacyExcludedErrorCodeAsIgnored() {
        Configuration configuration = new Configuration();
        configuration.set(SINK_HTTP_IGNORED_RESPONSE_CODES, "404");

        Properties properties = new Properties();
        properties.setProperty(HttpConnectorConfigConstants.HTTP_ERROR_SINK_CODES_LIST, "4XX");
        properties.setProperty(
                HttpConnectorConfigConstants.HTTP_ERROR_SINK_CODE_INCLUDE_LIST, "404");

        HttpSinkResponseClassifier classifier =
                new HttpSinkResponseClassifier(config(configuration, properties));

        assertThat(classifier.classify(response(404))).isEqualTo(HttpSinkResponseStatus.IGNORED);
        assertThat(classifier.classify(response(400)))
                .isEqualTo(HttpSinkResponseStatus.FATAL_FAILURE);
    }

    @Test
    void shouldUseLegacyDefaultsWhenOnlyLegacyExcludedErrorCodeIsConfigured() {
        Configuration configuration = new Configuration();
        configuration.set(SINK_HTTP_IGNORED_RESPONSE_CODES, "404");

        Properties properties = new Properties();
        properties.setProperty(
                HttpConnectorConfigConstants.HTTP_ERROR_SINK_CODE_INCLUDE_LIST, "404");

        HttpSinkResponseClassifier classifier =
                new HttpSinkResponseClassifier(config(configuration, properties));

        assertThat(classifier.classify(response(404))).isEqualTo(HttpSinkResponseStatus.IGNORED);
        assertThat(classifier.classify(response(500)))
                .isEqualTo(HttpSinkResponseStatus.FATAL_FAILURE);
    }

    @Test
    void shouldRejectLegacyErrorCodeTogetherWithSuccessCodes() {
        Configuration configuration = new Configuration();
        configuration.set(SINK_HTTP_SUCCESS_CODES, "2XX");

        Properties properties = new Properties();
        properties.setProperty(HttpConnectorConfigConstants.HTTP_ERROR_SINK_CODES_LIST, "4XX");

        assertThatThrownBy(() -> new HttpSinkResponseClassifier(config(configuration, properties)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Cannot set legacy HTTP sink error-code properties")
                .hasMessageContaining(SINK_HTTP_SUCCESS_CODES.key())
                .hasMessageNotContaining(SINK_HTTP_RETRY_CODES.key());
    }

    @Test
    void shouldRejectLegacyErrorCodeTogetherWithRetryCodes() {
        Configuration configuration = new Configuration();
        configuration.set(SINK_HTTP_RETRY_CODES, "500,503");

        Properties properties = new Properties();
        properties.setProperty(HttpConnectorConfigConstants.HTTP_ERROR_SINK_CODES_LIST, "4XX");

        assertThatThrownBy(() -> new HttpSinkResponseClassifier(config(configuration, properties)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Cannot set legacy HTTP sink error-code properties")
                .hasMessageContaining(SINK_HTTP_RETRY_CODES.key())
                .hasMessageNotContaining(SINK_HTTP_SUCCESS_CODES.key());
    }

    @Test
    void shouldRejectLegacyExcludedErrorCodeTogetherWithSuccessCodes() {
        Configuration configuration = new Configuration();
        configuration.set(SINK_HTTP_SUCCESS_CODES, "2XX,404");

        Properties properties = new Properties();
        properties.setProperty(
                HttpConnectorConfigConstants.HTTP_ERROR_SINK_CODE_INCLUDE_LIST, "404");

        assertThatThrownBy(() -> new HttpSinkResponseClassifier(config(configuration, properties)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Cannot set legacy HTTP sink error-code properties")
                .hasMessageContaining(
                        HttpConnectorConfigConstants.HTTP_ERROR_SINK_CODE_INCLUDE_LIST)
                .hasMessageContaining(SINK_HTTP_SUCCESS_CODES.key())
                .hasMessageNotContaining(SINK_HTTP_RETRY_CODES.key());
    }

    @Test
    void shouldRejectLegacyExcludedErrorCodeTogetherWithRetryCodes() {
        Configuration configuration = new Configuration();
        configuration.set(SINK_HTTP_RETRY_CODES, "500,503");

        Properties properties = new Properties();
        properties.setProperty(
                HttpConnectorConfigConstants.HTTP_ERROR_SINK_CODE_INCLUDE_LIST, "404");

        assertThatThrownBy(() -> new HttpSinkResponseClassifier(config(configuration, properties)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Cannot set legacy HTTP sink error-code properties")
                .hasMessageContaining(
                        HttpConnectorConfigConstants.HTTP_ERROR_SINK_CODE_INCLUDE_LIST)
                .hasMessageContaining(SINK_HTTP_RETRY_CODES.key())
                .hasMessageNotContaining(SINK_HTTP_SUCCESS_CODES.key());
    }

    @Test
    void shouldAllowLegacyErrorCodeTogetherWithIgnoredResponseCodes() {
        Configuration configuration = new Configuration();
        configuration.set(SINK_HTTP_IGNORED_RESPONSE_CODES, "404");

        Properties properties = new Properties();
        properties.setProperty(HttpConnectorConfigConstants.HTTP_ERROR_SINK_CODES_LIST, "4XX");
        properties.setProperty(SINK_HTTP_IGNORED_RESPONSE_CODES.key(), "404");

        HttpSinkResponseClassifier classifier =
                new HttpSinkResponseClassifier(config(configuration, properties));

        assertThat(classifier.classify(response(404))).isEqualTo(HttpSinkResponseStatus.IGNORED);
        assertThat(classifier.classify(response(400)))
                .isEqualTo(HttpSinkResponseStatus.FATAL_FAILURE);
        assertThat(classifier.classify(response(200))).isEqualTo(HttpSinkResponseStatus.SUCCESS);
    }

    @Test
    void shouldRejectLegacyExcludedErrorCodeTogetherWithIgnoredResponseCodes() {
        Properties properties = new Properties();
        properties.setProperty(
                HttpConnectorConfigConstants.HTTP_ERROR_SINK_CODE_INCLUDE_LIST, "404");
        properties.setProperty(SINK_HTTP_IGNORED_RESPONSE_CODES.key(), "409");

        assertThatThrownBy(
                        () ->
                                new HttpSinkResponseClassifier(
                                        config(new Configuration(), properties)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        HttpConnectorConfigConstants.HTTP_ERROR_SINK_CODE_INCLUDE_LIST)
                .hasMessageContaining(SINK_HTTP_IGNORED_RESPONSE_CODES.key());
    }

    @Test
    void shouldRejectLegacyErrorCodeTogetherWithBothNewOptions() {
        Configuration configuration = new Configuration();
        configuration.set(SINK_HTTP_SUCCESS_CODES, "2XX");
        configuration.set(SINK_HTTP_RETRY_CODES, "500,503");

        Properties properties = new Properties();
        properties.setProperty(HttpConnectorConfigConstants.HTTP_ERROR_SINK_CODES_LIST, "4XX");

        assertThatThrownBy(() -> new HttpSinkResponseClassifier(config(configuration, properties)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Cannot set legacy HTTP sink error-code properties")
                .hasMessageContaining(SINK_HTTP_SUCCESS_CODES.key())
                .hasMessageContaining(SINK_HTTP_RETRY_CODES.key());
    }

    private static HttpSinkConfig config(Configuration configuration) {
        return config(configuration, new Properties());
    }

    private static HttpSinkConfig config(Configuration configuration, Properties properties) {
        return HttpSinkConfig.builder()
                .url("http://localhost")
                .properties(properties)
                .readableConfig(configuration)
                .httpPostRequestCallback(new Slf4jHttpPostRequestCallback())
                .build();
    }

    private static HttpResponse<String> response(int statusCode) {
        HttpResponse<String> response = mock(HttpResponse.class);
        when(response.statusCode()).thenReturn(statusCode);
        return response;
    }
}
