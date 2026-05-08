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

package org.apache.flink.connector.http.sink;

import org.apache.flink.connector.http.config.HttpConnectorConfigConstants;
import org.apache.flink.connector.http.retry.RetryStrategyType;
import org.apache.flink.connector.http.status.HttpCodesParser;
import org.apache.flink.connector.http.status.HttpResponseChecker;
import org.apache.flink.util.ConfigurationException;
import org.apache.flink.util.TimeUtils;

import io.github.resilience4j.core.IntervalFunction;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.Properties;

/**
 * Parsed retry configuration for the HTTP sink.
 *
 * <p>Mirrors the lookup source retry configuration (strategy type, retry codes, success codes,
 * fixed / exponential delay parameters) and is consumed from the sink-builder {@link Properties}
 * bag so that both the Table API and the DataStream {@code HttpSinkBuilder} can share the same
 * knobs.
 */
@Slf4j
@Getter
final class SinkRetryConfig {

    static final String DEFAULT_STRATEGY = RetryStrategyType.EXPONENTIAL_DELAY.getCode();
    static final String DEFAULT_SUCCESS_CODES = "2XX";
    static final String DEFAULT_RETRY_CODES = "500,503,504";
    static final Duration DEFAULT_FIXED_DELAY = Duration.ofSeconds(1);
    static final Duration DEFAULT_EXP_INITIAL_BACKOFF = Duration.ofSeconds(1);
    static final Duration DEFAULT_EXP_MAX_BACKOFF = Duration.ofMinutes(1);
    static final double DEFAULT_EXP_MULTIPLIER = 2.0d;
    static final int DEFAULT_MAX_RETRIES = 3;

    private final int maxRetries;
    private final HttpResponseChecker responseChecker;
    private final IntervalFunction intervalFunction;

    private SinkRetryConfig(
            int maxRetries,
            HttpResponseChecker responseChecker,
            IntervalFunction intervalFunction) {
        this.maxRetries = maxRetries;
        this.responseChecker = responseChecker;
        this.intervalFunction = intervalFunction;
    }

    /**
     * Compute the backoff (in milliseconds) for a zero-based {@code attempt} index (0 is the first
     * retry, 1 is the second, ...). resilience4j's {@link IntervalFunction} is 1-based internally,
     * hence the {@code +1}.
     */
    long backoffMillis(int attempt) {
        // IntervalFunction is 1-based: attempt=0 ⇒ apply(1) gives the first retry delay, etc.
        return intervalFunction.apply(attempt + 1);
    }

    /** Whether the given HTTP status code should trigger a retry. */
    boolean shouldRetry(int statusCode) {
        return responseChecker.isTemporalError(statusCode);
    }

    /** Whether the given HTTP status code represents a success. */
    boolean isSuccess(int statusCode) {
        return responseChecker.isSuccessful(statusCode);
    }

    static SinkRetryConfig fromProperties(Properties properties) {
        int maxRetries =
                getInt(
                        properties,
                        HttpConnectorConfigConstants.SINK_HTTP_RETRY_TIMES,
                        DEFAULT_MAX_RETRIES);
        HttpResponseChecker checker = buildChecker(properties);
        IntervalFunction intervalFunction = buildIntervalFunction(properties);
        return new SinkRetryConfig(maxRetries, checker, intervalFunction);
    }

    private static HttpResponseChecker buildChecker(Properties properties) {
        String successCodes =
                properties.getProperty(
                        HttpConnectorConfigConstants.SINK_RETRY_SUCCESS_CODES,
                        DEFAULT_SUCCESS_CODES);
        String retryCodes =
                properties.getProperty(
                        HttpConnectorConfigConstants.SINK_RETRY_RETRY_CODES, DEFAULT_RETRY_CODES);
        try {
            return new HttpResponseChecker(
                    HttpCodesParser.parse(successCodes), HttpCodesParser.parse(retryCodes));
        } catch (ConfigurationException e) {
            throw new IllegalArgumentException(
                    "Invalid HTTP sink retry codes configuration: " + e.getMessage(), e);
        }
    }

    private static IntervalFunction buildIntervalFunction(Properties properties) {
        String rawStrategy =
                properties.getProperty(
                        HttpConnectorConfigConstants.SINK_RETRY_STRATEGY_TYPE, DEFAULT_STRATEGY);
        RetryStrategyType strategy = RetryStrategyType.fromCode(rawStrategy);
        switch (strategy) {
            case FIXED_DELAY:
                {
                    Duration delay =
                            getDuration(
                                    properties,
                                    HttpConnectorConfigConstants.SINK_RETRY_FIXED_DELAY_DELAY,
                                    DEFAULT_FIXED_DELAY);
                    return IntervalFunction.of(delay);
                }
            case EXPONENTIAL_DELAY:
                {
                    Duration initial =
                            getDuration(
                                    properties,
                                    HttpConnectorConfigConstants
                                            .SINK_RETRY_EXP_DELAY_INITIAL_BACKOFF,
                                    DEFAULT_EXP_INITIAL_BACKOFF);
                    Duration max =
                            getDuration(
                                    properties,
                                    HttpConnectorConfigConstants.SINK_RETRY_EXP_DELAY_MAX_BACKOFF,
                                    DEFAULT_EXP_MAX_BACKOFF);
                    double multiplier =
                            getDouble(
                                    properties,
                                    HttpConnectorConfigConstants.SINK_RETRY_EXP_DELAY_MULTIPLIER,
                                    DEFAULT_EXP_MULTIPLIER);
                    return IntervalFunction.ofExponentialBackoff(initial, multiplier, max);
                }
            default:
                throw new IllegalArgumentException("Unsupported retry strategy: " + strategy);
        }
    }

    private static int getInt(Properties properties, String key, int defaultValue) {
        String raw = properties.getProperty(key);
        if (raw == null || raw.isBlank()) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(raw.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    "Invalid integer value for '" + key + "': " + raw, e);
        }
    }

    private static double getDouble(Properties properties, String key, double defaultValue) {
        String raw = properties.getProperty(key);
        if (raw == null || raw.isBlank()) {
            return defaultValue;
        }
        try {
            return Double.parseDouble(raw.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid double value for '" + key + "': " + raw, e);
        }
    }

    private static Duration getDuration(Properties properties, String key, Duration defaultValue) {
        String raw = properties.getProperty(key);
        if (raw == null || raw.isBlank()) {
            return defaultValue;
        }
        String trimmed = raw.trim();
        // Accept both Flink's human-friendly format (e.g. "1s", "1min") and the ISO-8601 format
        // produced by Duration#toString (e.g. "PT1S"). The latter is what HttpDynamicSink emits
        // when it serialises a ConfigOption<Duration> value back into the sink properties bag.
        try {
            return TimeUtils.parseDuration(trimmed);
        } catch (Exception flinkParseError) {
            try {
                return Duration.parse(trimmed);
            } catch (Exception isoParseError) {
                IllegalArgumentException ex =
                        new IllegalArgumentException(
                                "Invalid duration value for '" + key + "': " + raw,
                                flinkParseError);
                ex.addSuppressed(isoParseError);
                throw ex;
            }
        }
    }
}
