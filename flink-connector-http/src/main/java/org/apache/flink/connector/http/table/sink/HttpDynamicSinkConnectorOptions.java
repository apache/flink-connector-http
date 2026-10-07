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

package org.apache.flink.connector.http.table.sink;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.connector.http.config.HttpConnectorConfigConstants;
import org.apache.flink.connector.http.retry.RetryStrategyType;

import java.time.Duration;

import static org.apache.flink.connector.http.config.HttpConnectorConfigConstants.SINK_HTTP_TIMEOUT_SECONDS;
import static org.apache.flink.connector.http.config.HttpConnectorConfigConstants.SINK_HTTP_WRITER_THREAD_POOL_SIZE;
import static org.apache.flink.connector.http.config.HttpConnectorConfigConstants.SINK_IGNORE_RESPONSE_CODES;
import static org.apache.flink.connector.http.config.HttpConnectorConfigConstants.SINK_REQUEST_CALLBACK_IDENTIFIER;
import static org.apache.flink.connector.http.config.HttpConnectorConfigConstants.SINK_RETRY_CODES;
import static org.apache.flink.connector.http.config.HttpConnectorConfigConstants.SINK_RETRY_EXP_DELAY_INITIAL_BACKOFF;
import static org.apache.flink.connector.http.config.HttpConnectorConfigConstants.SINK_RETRY_EXP_DELAY_MAX_BACKOFF;
import static org.apache.flink.connector.http.config.HttpConnectorConfigConstants.SINK_RETRY_EXP_DELAY_MULTIPLIER;
import static org.apache.flink.connector.http.config.HttpConnectorConfigConstants.SINK_RETRY_STRATEGY_TYPE;
import static org.apache.flink.connector.http.config.HttpConnectorConfigConstants.SINK_SUCCESS_CODES;

/** Table API options for {@link HttpDynamicSink}. */
public class HttpDynamicSinkConnectorOptions {

    public static final ConfigOption<String> URL =
            ConfigOptions.key("url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The HTTP endpoint URL.");

    public static final ConfigOption<String> INSERT_METHOD =
            ConfigOptions.key("insert-method")
                    .stringType()
                    .defaultValue("POST")
                    .withDescription("Method used for requests built from SQL's INSERT.");

    /**
     * HTTP request timeout for sink. Controls how long the HTTP client waits for a response before
     * timing out a single request. Defaults to 30 seconds.
     */
    public static final ConfigOption<Duration> SINK_REQUEST_TIMEOUT =
            ConfigOptions.key(SINK_HTTP_TIMEOUT_SECONDS)
                    .durationType()
                    .defaultValue(Duration.ofSeconds(30))
                    .withDescription(
                            "HTTP request timeout for sink. "
                                    + "Controls how long the HTTP client waits for a response "
                                    + "before timing out a single request. "
                                    + "Specified as a Duration, e.g. '30s' or '1min'.");

    public static final ConfigOption<String> REQUEST_CALLBACK_IDENTIFIER =
            ConfigOptions.key(SINK_REQUEST_CALLBACK_IDENTIFIER)
                    .stringType()
                    .defaultValue(Slf4jHttpPostRequestCallbackFactory.IDENTIFIER);

    /**
     * Thread pool size for HTTP sink writer response handling. Defaults to 4 threads when not
     * specified, matching the previous runtime default.
     */
    public static final ConfigOption<Integer> SINK_WRITER_THREAD_POOL_SIZE =
            ConfigOptions.key(SINK_HTTP_WRITER_THREAD_POOL_SIZE)
                    .intType()
                    .defaultValue(4)
                    .withDescription(
                            "Sets the size of the thread pool for HTTP sink request processing. "
                                    + "Defaults to 4 when this option is unset.");

    public static final ConfigOption<Integer> SINK_MAX_RETRIES =
            ConfigOptions.key(HttpConnectorConfigConstants.SINK_MAX_RETRIES)
                    .intType()
                    .defaultValue(0)
                    .withDescription(
                            "The maximum number of retries for failed HTTP sink requests. "
                                    + "Defaults to 0 (retries disabled) so existing jobs keep "
                                    + "send-once behaviour. Set a positive value to enable retries.");

    public static final ConfigOption<String> SINK_HTTP_SUCCESS_CODES =
            ConfigOptions.key(SINK_SUCCESS_CODES)
                    .stringType()
                    .defaultValue("2XX")
                    .withDescription(
                            "Comma separated HTTP status codes considered as successful sink responses. "
                                    + "Use [1-5]XX for groups and '!' for exclusions. "
                                    + "Cannot be set together with legacy http.sink.error.code properties.");

    public static final ConfigOption<String> SINK_HTTP_RETRY_CODES =
            ConfigOptions.key(SINK_RETRY_CODES)
                    .stringType()
                    .defaultValue("500,503,504")
                    .withDescription(
                            "Comma separated HTTP status codes considered as retryable sink responses. "
                                    + "Use [1-5]XX for groups and '!' for exclusions. "
                                    + "Cannot be set together with legacy http.sink.error.code properties.");

    public static final ConfigOption<String> SINK_HTTP_IGNORED_RESPONSE_CODES =
            ConfigOptions.key(SINK_IGNORE_RESPONSE_CODES)
                    .stringType()
                    .defaultValue("")
                    .withDescription(
                            "Comma separated HTTP status codes that should be treated as successful "
                                    + "without retrying. Cannot be set together with legacy "
                                    + "http.sink.error.code.exclude.");

    public static final ConfigOption<String> SINK_RETRY_STRATEGY =
            ConfigOptions.key(SINK_RETRY_STRATEGY_TYPE)
                    .stringType()
                    .defaultValue(RetryStrategyType.FIXED_DELAY.getCode())
                    .withDescription(
                            "HTTP sink retry strategy type: fixed-delay (default) or exponential-delay.");

    public static final ConfigOption<Duration> SINK_RETRY_FIXED_DELAY_DELAY =
            ConfigOptions.key(HttpConnectorConfigConstants.SINK_RETRY_FIXED_DELAY_DELAY)
                    .durationType()
                    .defaultValue(Duration.ofSeconds(1))
                    .withDescription("Fixed-delay interval between HTTP sink retries.");

    public static final ConfigOption<Duration> SINK_RETRY_EXPONENTIAL_DELAY_INITIAL_BACKOFF =
            ConfigOptions.key(SINK_RETRY_EXP_DELAY_INITIAL_BACKOFF)
                    .durationType()
                    .defaultValue(Duration.ofSeconds(1))
                    .withDescription("Exponential-delay initial backoff for HTTP sink retries.");

    public static final ConfigOption<Duration> SINK_RETRY_EXPONENTIAL_DELAY_MAX_BACKOFF =
            ConfigOptions.key(SINK_RETRY_EXP_DELAY_MAX_BACKOFF)
                    .durationType()
                    .defaultValue(Duration.ofMinutes(1))
                    .withDescription("Exponential-delay maximum backoff for HTTP sink retries.");

    public static final ConfigOption<Double> SINK_RETRY_EXPONENTIAL_DELAY_MULTIPLIER =
            ConfigOptions.key(SINK_RETRY_EXP_DELAY_MULTIPLIER)
                    .doubleType()
                    .defaultValue(1.5)
                    .withDescription("Exponential-delay backoff multiplier for HTTP sink retries.");
}
