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

package org.apache.flink.connector.http.table.sink;

import org.apache.flink.connector.http.config.HttpConnectorConfigConstants;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Unfortunately it seems that Flink is lazy with connector instantiation, so one has to call INSERT
 * in order to test the Factory.
 */
public class HttpDynamicTableSinkFactoryTest {

    protected StreamExecutionEnvironment env;
    protected StreamTableEnvironment tEnv;

    @BeforeEach
    public void setup() {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        tEnv = StreamTableEnvironment.create(env);
    }

    @Test
    public void requiredOptionsTest() {
        final String noFormatOptionCreate =
                String.format(
                        "CREATE TABLE formatHttp (\n"
                                + "  id bigint\n"
                                + ") with (\n"
                                + "  'connector' = '%s',\n"
                                + "  'url' = '%s'\n"
                                + ")",
                        HttpDynamicTableSinkFactory.IDENTIFIER, "http://localhost/");
        tEnv.executeSql(noFormatOptionCreate);
        assertThatThrownBy(() -> tEnv.executeSql("INSERT INTO formatHttp VALUES (1)").await())
                .isInstanceOf(ValidationException.class);

        final String noUrlOptionCreate =
                String.format(
                        "CREATE TABLE urlHttp (\n"
                                + "  id bigint\n"
                                + ") with (\n"
                                + "  'connector' = '%s',\n"
                                + "  'format' = 'json'\n"
                                + ")",
                        HttpDynamicTableSinkFactory.IDENTIFIER);
        tEnv.executeSql(noUrlOptionCreate);
        assertThatThrownBy(() -> tEnv.executeSql("INSERT INTO urlHttp VALUES (1)").await())
                .isInstanceOf(ValidationException.class);
    }

    @Test
    public void validateHttpSinkOptionsTest() {
        final String invalidInsertMethod =
                String.format(
                        "CREATE TABLE http (\n"
                                + "  id bigint\n"
                                + ") with (\n"
                                + "  'connector' = '%s',\n"
                                + "  'url' = '%s',\n"
                                + "  'format' = 'json',\n"
                                + "  'insert-method' = 'GET'\n"
                                + ")",
                        HttpDynamicTableSinkFactory.IDENTIFIER, "http://localhost/");
        tEnv.executeSql(invalidInsertMethod);
        assertThatThrownBy(() -> tEnv.executeSql("INSERT INTO http VALUES (1)").await())
                .isInstanceOf(ValidationException.class);
    }

    @Test
    public void nonexistentOptionsTest() {
        final String invalidInsertMethod =
                String.format(
                        "CREATE TABLE http (\n"
                                + "  id bigint\n"
                                + ") with (\n"
                                + "  'connector' = '%s',\n"
                                + "  'url' = '%s',\n"
                                + "  'format' = 'json',\n"
                                + "  'some-random-totally-unexisting-option-!g*Av#' = '7123'\n"
                                + ")",
                        HttpDynamicTableSinkFactory.IDENTIFIER, "http://localhost/");
        tEnv.executeSql(invalidInsertMethod);
        assertThatThrownBy(() -> tEnv.executeSql("INSERT INTO http VALUES (1)").await())
                .isInstanceOf(ValidationException.class);
    }

    @Test
    public void validateWriterThreadPoolSizeTest() {
        final String invalidThreadPoolSize =
                String.format(
                        "CREATE TABLE http (\n"
                                + "  id bigint\n"
                                + ") with (\n"
                                + "  'connector' = '%s',\n"
                                + "  'url' = '%s',\n"
                                + "  'format' = 'json',\n"
                                + "  '%s' = '0'\n"
                                + ")",
                        HttpDynamicTableSinkFactory.IDENTIFIER,
                        "http://localhost/",
                        HttpDynamicSinkConnectorOptions.SINK_WRITER_THREAD_POOL_SIZE.key());
        tEnv.executeSql(invalidThreadPoolSize);
        assertThatThrownBy(() -> tEnv.executeSql("INSERT INTO http VALUES (1)").await())
                .isInstanceOf(ValidationException.class);
    }

    @Test
    public void requestTimeoutOptionTest() {
        // Verify that http.sink.request.timeout is a valid recognized option (no
        // ValidationException)
        final String withRequestTimeout =
                String.format(
                        "CREATE TABLE httpTimeout (\n"
                                + "  id bigint\n"
                                + ") with (\n"
                                + "  'connector' = '%s',\n"
                                + "  'url' = '%s',\n"
                                + "  'format' = 'json',\n"
                                + "  '%s' = '60s'\n"
                                + ")",
                        HttpDynamicTableSinkFactory.IDENTIFIER,
                        "http://localhost/",
                        HttpDynamicSinkConnectorOptions.SINK_REQUEST_TIMEOUT.key());
        tEnv.executeSql(withRequestTimeout);
        // The duration-typed option must be recognized at DDL time; INSERT may fail at runtime
        // (e.g. no HTTP server) but must not raise ValidationException.
        Throwable insertFailure =
                catchThrowable(() -> tEnv.executeSql("INSERT INTO httpTimeout VALUES (1)").await());
        if (insertFailure != null) {
            assertThat(insertFailure)
                    .as("request.timeout must be a recognized option")
                    .isNotInstanceOf(ValidationException.class);
        }
    }

    @Test
    public void validateMaxRetriesTest() {
        final String invalidMaxRetries =
                String.format(
                        "CREATE TABLE httpRetries (\n"
                                + "  id bigint\n"
                                + ") with (\n"
                                + "  'connector' = '%s',\n"
                                + "  'url' = '%s',\n"
                                + "  'format' = 'json',\n"
                                + "  '%s' = '-1'\n"
                                + ")",
                        HttpDynamicTableSinkFactory.IDENTIFIER,
                        "http://localhost/",
                        HttpDynamicSinkConnectorOptions.SINK_MAX_RETRIES.key());
        tEnv.executeSql(invalidMaxRetries);
        assertThatThrownBy(() -> tEnv.executeSql("INSERT INTO httpRetries VALUES (1)").await())
                .isInstanceOf(ValidationException.class);
    }

    @Test
    public void validateSinkStatusCodeOptionsTest() {
        final String overlappingStatusCodes =
                String.format(
                        "CREATE TABLE httpStatusCodes (\n"
                                + "  id bigint\n"
                                + ") with (\n"
                                + "  'connector' = '%s',\n"
                                + "  'url' = '%s',\n"
                                + "  'format' = 'json',\n"
                                + "  '%s' = '2XX,500',\n"
                                + "  '%s' = '500'\n"
                                + ")",
                        HttpDynamicTableSinkFactory.IDENTIFIER,
                        "http://localhost/",
                        HttpDynamicSinkConnectorOptions.SINK_HTTP_SUCCESS_CODES.key(),
                        HttpDynamicSinkConnectorOptions.SINK_HTTP_RETRY_CODES.key());
        tEnv.executeSql(overlappingStatusCodes);
        assertThatThrownBy(() -> tEnv.executeSql("INSERT INTO httpStatusCodes VALUES (1)").await())
                .isInstanceOf(ValidationException.class);
    }

    @Test
    public void ignoredResponseCodesOverrideDefaultRetryCodesTest() {
        final String ignoredRetryCode =
                String.format(
                        "CREATE TABLE httpIgnoredRetryCode (\n"
                                + "  id bigint\n"
                                + ") with (\n"
                                + "  'connector' = '%s',\n"
                                + "  'url' = '%s',\n"
                                + "  'format' = 'json',\n"
                                + "  '%s' = '500'\n"
                                + ")",
                        HttpDynamicTableSinkFactory.IDENTIFIER,
                        "http://localhost/",
                        HttpDynamicSinkConnectorOptions.SINK_HTTP_IGNORED_RESPONSE_CODES.key());
        tEnv.executeSql(ignoredRetryCode);
        Throwable insertFailure =
                catchThrowable(
                        () ->
                                tEnv.executeSql("INSERT INTO httpIgnoredRetryCode VALUES (1)")
                                        .await());
        if (insertFailure != null) {
            assertThat(insertFailure)
                    .as("ignored response codes must override default retry codes")
                    .isNotInstanceOf(ValidationException.class);
        }
    }

    @Test
    public void validateRetryFixedDelayDurationTest() {
        assertInvalidRetryOption(
                "httpFixedDelay",
                String.format(
                        "  '%s' = '0s'\n",
                        HttpDynamicSinkConnectorOptions.SINK_RETRY_FIXED_DELAY_DELAY.key()));
    }

    @Test
    public void validateRetryExponentialInitialBackoffDurationTest() {
        assertInvalidRetryOption(
                "httpInitialBackoff",
                String.format(
                        "  '%s' = '0s'\n",
                        HttpDynamicSinkConnectorOptions.SINK_RETRY_EXPONENTIAL_DELAY_INITIAL_BACKOFF
                                .key()));
    }

    @Test
    public void validateRetryExponentialMaxBackoffDurationTest() {
        assertInvalidRetryOption(
                "httpMaxBackoff",
                String.format(
                        "  '%s' = '0s'\n",
                        HttpDynamicSinkConnectorOptions.SINK_RETRY_EXPONENTIAL_DELAY_MAX_BACKOFF
                                .key()));
    }

    @Test
    public void validateRetryExponentialMaxBackoffRangeTest() {
        assertInvalidRetryOption(
                "httpBackoffRange",
                String.format(
                        "  '%s' = '10s',\n" + "  '%s' = '1s'\n",
                        HttpDynamicSinkConnectorOptions.SINK_RETRY_EXPONENTIAL_DELAY_INITIAL_BACKOFF
                                .key(),
                        HttpDynamicSinkConnectorOptions.SINK_RETRY_EXPONENTIAL_DELAY_MAX_BACKOFF
                                .key()));
    }

    @Test
    public void acceptsSinkRetryAndStatusCodeOptionsTest() {
        final String retryOptions =
                String.format(
                        "CREATE TABLE httpRetryOptions (\n"
                                + "  id bigint\n"
                                + ") with (\n"
                                + "  'connector' = '%s',\n"
                                + "  'url' = '%s',\n"
                                + "  'format' = 'json',\n"
                                + "  '%s' = '2',\n"
                                + "  '%s' = '2XX',\n"
                                + "  '%s' = '500,503',\n"
                                + "  '%s' = '404',\n"
                                + "  '%s' = 'exponential-delay',\n"
                                + "  '%s' = '1s',\n"
                                + "  '%s' = '1s',\n"
                                + "  '%s' = '30s',\n"
                                + "  '%s' = '2.0'\n"
                                + ")",
                        HttpDynamicTableSinkFactory.IDENTIFIER,
                        "http://localhost/",
                        HttpDynamicSinkConnectorOptions.SINK_MAX_RETRIES.key(),
                        HttpDynamicSinkConnectorOptions.SINK_HTTP_SUCCESS_CODES.key(),
                        HttpDynamicSinkConnectorOptions.SINK_HTTP_RETRY_CODES.key(),
                        HttpDynamicSinkConnectorOptions.SINK_HTTP_IGNORED_RESPONSE_CODES.key(),
                        HttpDynamicSinkConnectorOptions.SINK_RETRY_STRATEGY.key(),
                        HttpDynamicSinkConnectorOptions.SINK_RETRY_FIXED_DELAY_DELAY.key(),
                        HttpDynamicSinkConnectorOptions.SINK_RETRY_EXPONENTIAL_DELAY_INITIAL_BACKOFF
                                .key(),
                        HttpDynamicSinkConnectorOptions.SINK_RETRY_EXPONENTIAL_DELAY_MAX_BACKOFF
                                .key(),
                        HttpDynamicSinkConnectorOptions.SINK_RETRY_EXPONENTIAL_DELAY_MULTIPLIER
                                .key());
        tEnv.executeSql(retryOptions);
        Throwable insertFailure =
                catchThrowable(
                        () -> tEnv.executeSql("INSERT INTO httpRetryOptions VALUES (1)").await());
        if (insertFailure != null) {
            assertThat(insertFailure)
                    .as("sink retry/status options must be recognized")
                    .isNotInstanceOf(ValidationException.class);
        }
    }

    @Test
    public void rejectLegacyErrorCodeTogetherWithSuccessCodesTest() {
        assertInvalidRetryOption(
                "httpMixedStatusCodes",
                String.format(
                        "  '%s' = '4XX',\n" + "  '%s' = '2XX'\n",
                        HttpConnectorConfigConstants.HTTP_ERROR_SINK_CODES_LIST,
                        HttpDynamicSinkConnectorOptions.SINK_HTTP_SUCCESS_CODES.key()));
    }

    @Test
    public void rejectLegacyErrorCodeTogetherWithRetryCodesTest() {
        assertInvalidRetryOption(
                "httpMixedRetryCodes",
                String.format(
                        "  '%s' = '4XX',\n" + "  '%s' = '500,503'\n",
                        HttpConnectorConfigConstants.HTTP_ERROR_SINK_CODES_LIST,
                        HttpDynamicSinkConnectorOptions.SINK_HTTP_RETRY_CODES.key()));
    }

    @Test
    public void rejectLegacyExcludeTogetherWithRetryCodesTest() {
        assertInvalidRetryOption(
                "httpMixedExcludeRetryCodes",
                String.format(
                        "  '%s' = '404',\n" + "  '%s' = '500,503'\n",
                        HttpConnectorConfigConstants.HTTP_ERROR_SINK_CODE_INCLUDE_LIST,
                        HttpDynamicSinkConnectorOptions.SINK_HTTP_RETRY_CODES.key()));
    }

    @Test
    public void rejectLegacyExcludeTogetherWithIgnoredResponseCodesTest() {
        assertInvalidRetryOption(
                "httpMixedIgnoredCodes",
                String.format(
                        "  '%s' = '404',\n" + "  '%s' = '409'\n",
                        HttpConnectorConfigConstants.HTTP_ERROR_SINK_CODE_INCLUDE_LIST,
                        HttpDynamicSinkConnectorOptions.SINK_HTTP_IGNORED_RESPONSE_CODES.key()));
    }

    @Test
    public void acceptsLegacyErrorCodeTogetherWithIgnoredResponseCodesTest() {
        final String ddl =
                String.format(
                        "CREATE TABLE httpLegacyErrorWithIgnored (\n"
                                + "  id bigint\n"
                                + ") with (\n"
                                + "  'connector' = '%s',\n"
                                + "  'url' = '%s',\n"
                                + "  'format' = 'json',\n"
                                + "  '%s' = '4XX',\n"
                                + "  '%s' = '404'\n"
                                + ")",
                        HttpDynamicTableSinkFactory.IDENTIFIER,
                        "http://localhost/",
                        HttpConnectorConfigConstants.HTTP_ERROR_SINK_CODES_LIST,
                        HttpDynamicSinkConnectorOptions.SINK_HTTP_IGNORED_RESPONSE_CODES.key());
        tEnv.executeSql(ddl);
        Throwable insertFailure =
                catchThrowable(
                        () ->
                                tEnv.executeSql("INSERT INTO httpLegacyErrorWithIgnored VALUES (1)")
                                        .await());
        if (insertFailure != null) {
            assertThat(insertFailure)
                    .as("legacy error.code with ignored-response-codes must be accepted")
                    .isNotInstanceOf(ValidationException.class);
        }
    }

    private void assertInvalidRetryOption(String tableName, String retryOptionLine) {
        final String ddl =
                String.format(
                        "CREATE TABLE %s (\n"
                                + "  id bigint\n"
                                + ") with (\n"
                                + "  'connector' = '%s',\n"
                                + "  'url' = '%s',\n"
                                + "  'format' = 'json',\n"
                                + "%s"
                                + ")",
                        tableName,
                        HttpDynamicTableSinkFactory.IDENTIFIER,
                        "http://localhost/",
                        retryOptionLine);
        tEnv.executeSql(ddl);
        assertThatThrownBy(
                        () -> tEnv.executeSql("INSERT INTO " + tableName + " VALUES (1)").await())
                .isInstanceOf(ValidationException.class);
    }
}
