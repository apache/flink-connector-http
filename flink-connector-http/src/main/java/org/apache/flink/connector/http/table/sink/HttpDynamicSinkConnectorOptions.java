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
import org.apache.flink.connector.http.retry.RetryStrategyType;

import java.time.Duration;

import static org.apache.flink.connector.http.config.HttpConnectorConfigConstants.SINK_HTTP_RETRY_TIMES;
import static org.apache.flink.connector.http.config.HttpConnectorConfigConstants.SINK_HTTP_TIMEOUT_SECONDS;
import static org.apache.flink.connector.http.config.HttpConnectorConfigConstants.SINK_REQUEST_CALLBACK_IDENTIFIER;
import static org.apache.flink.connector.http.config.HttpConnectorConfigConstants.SINK_RETRY_EXP_DELAY_INITIAL_BACKOFF;
import static org.apache.flink.connector.http.config.HttpConnectorConfigConstants.SINK_RETRY_EXP_DELAY_MAX_BACKOFF;
import static org.apache.flink.connector.http.config.HttpConnectorConfigConstants.SINK_RETRY_EXP_DELAY_MULTIPLIER;
import static org.apache.flink.connector.http.config.HttpConnectorConfigConstants.SINK_RETRY_FIXED_DELAY_DELAY;
import static org.apache.flink.connector.http.config.HttpConnectorConfigConstants.SINK_RETRY_RETRY_CODES;
import static org.apache.flink.connector.http.config.HttpConnectorConfigConstants.SINK_RETRY_STRATEGY_TYPE;
import static org.apache.flink.connector.http.config.HttpConnectorConfigConstants.SINK_RETRY_SUCCESS_CODES;

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

    // ---------- Retry configuration (mirrors the lookup source options) ----------

    public static final ConfigOption<Integer> RETRY_TIMES =
            ConfigOptions.key(SINK_HTTP_RETRY_TIMES)
                    .intType()
                    .defaultValue(3)
                    .withDescription(
                            "Maximum number of retry attempts for HTTP Sink requests on "
                                    + "IOException or on a transient HTTP status code (see "
                                    + SINK_RETRY_RETRY_CODES
                                    + "). Set to 0 to disable retries.");

    public static final ConfigOption<String> SINK_RETRY_STRATEGY =
            ConfigOptions.key(SINK_RETRY_STRATEGY_TYPE)
                    .stringType()
                    .defaultValue(RetryStrategyType.EXPONENTIAL_DELAY.getCode())
                    .withDescription(
                            "Auto retry strategy type for the sink: "
                                    + "fixed-delay or exponential-delay (default).");

    public static final ConfigOption<String> SINK_HTTP_SUCCESS_CODES =
            ConfigOptions.key(SINK_RETRY_SUCCESS_CODES)
                    .stringType()
                    .defaultValue("2XX")
                    .withDescription(
                            "Comma separated http codes considered as a successful sink response. "
                                    + "Use [1-5]XX for groups and '!' character for excluding.");

    public static final ConfigOption<String> SINK_HTTP_RETRY_CODES =
            ConfigOptions.key(SINK_RETRY_RETRY_CODES)
                    .stringType()
                    .defaultValue("500,503,504")
                    .withDescription(
                            "Comma separated http codes that will trigger a retry when returned by "
                                    + "the sink endpoint. Use [1-5]XX for groups and '!' character "
                                    + "for excluding.");

    public static final ConfigOption<Duration> SINK_RETRY_FIXED_DELAY =
            ConfigOptions.key(SINK_RETRY_FIXED_DELAY_DELAY)
                    .durationType()
                    .defaultValue(Duration.ofSeconds(1))
                    .withDescription("Fixed-delay interval between sink retries.");

    public static final ConfigOption<Duration> SINK_RETRY_EXPONENTIAL_DELAY_INITIAL_BACKOFF =
            ConfigOptions.key(SINK_RETRY_EXP_DELAY_INITIAL_BACKOFF)
                    .durationType()
                    .defaultValue(Duration.ofSeconds(1))
                    .withDescription("Exponential-delay initial delay for sink retries.");

    public static final ConfigOption<Duration> SINK_RETRY_EXPONENTIAL_DELAY_MAX_BACKOFF =
            ConfigOptions.key(SINK_RETRY_EXP_DELAY_MAX_BACKOFF)
                    .durationType()
                    .defaultValue(Duration.ofMinutes(1))
                    .withDescription("Exponential-delay maximum delay for sink retries.");

    public static final ConfigOption<Double> SINK_RETRY_EXPONENTIAL_DELAY_MULTIPLIER =
            ConfigOptions.key(SINK_RETRY_EXP_DELAY_MULTIPLIER)
                    .doubleType()
                    .defaultValue(2.0)
                    .withDescription("Exponential-delay multiplier for sink retries.");
}
