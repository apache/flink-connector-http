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

package org.apache.flink.connector.http.config;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.http.HttpPostRequestCallback;
import org.apache.flink.connector.http.sink.httpclient.HttpRequest;
import org.apache.flink.connector.http.table.sink.HttpDynamicSinkConnectorOptions;
import org.apache.flink.util.StringUtils;
import org.apache.flink.util.TimeUtils;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.apache.flink.connector.http.table.sink.HttpDynamicSinkConnectorOptions.SINK_HTTP_IGNORED_RESPONSE_CODES;
import static org.apache.flink.connector.http.table.sink.HttpDynamicSinkConnectorOptions.SINK_HTTP_RETRY_CODES;
import static org.apache.flink.connector.http.table.sink.HttpDynamicSinkConnectorOptions.SINK_HTTP_SUCCESS_CODES;
import static org.apache.flink.connector.http.table.sink.HttpDynamicSinkConnectorOptions.SINK_MAX_RETRIES;
import static org.apache.flink.connector.http.table.sink.HttpDynamicSinkConnectorOptions.SINK_REQUEST_TIMEOUT;
import static org.apache.flink.connector.http.table.sink.HttpDynamicSinkConnectorOptions.SINK_RETRY_EXPONENTIAL_DELAY_INITIAL_BACKOFF;
import static org.apache.flink.connector.http.table.sink.HttpDynamicSinkConnectorOptions.SINK_RETRY_EXPONENTIAL_DELAY_MAX_BACKOFF;
import static org.apache.flink.connector.http.table.sink.HttpDynamicSinkConnectorOptions.SINK_RETRY_EXPONENTIAL_DELAY_MULTIPLIER;
import static org.apache.flink.connector.http.table.sink.HttpDynamicSinkConnectorOptions.SINK_RETRY_FIXED_DELAY_DELAY;
import static org.apache.flink.connector.http.table.sink.HttpDynamicSinkConnectorOptions.SINK_RETRY_STRATEGY;
import static org.apache.flink.connector.http.table.sink.HttpDynamicSinkConnectorOptions.SINK_WRITER_THREAD_POOL_SIZE;

/** Factory helpers for {@link HttpSinkConfig}. */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class HttpSinkConfigFactory {

    public static HttpSinkConfig fromTableOptions(
            ReadableConfig readableConfig,
            Properties properties,
            HttpPostRequestCallback<HttpRequest> httpPostRequestCallback) {

        validateLegacyAndNewStatusCodeOptionsAreExclusive(readableConfig, properties);
        Configuration config =
                readableConfig instanceof Configuration
                        ? (Configuration) readableConfig
                        : Configuration.fromMap(readableConfig.toMap());
        setLegacyIgnoredResponseCodesIfNeeded(config, properties);

        return HttpSinkConfig.builder()
                .url(readableConfig.get(HttpDynamicSinkConnectorOptions.URL))
                .properties(properties)
                .readableConfig(config)
                .httpPostRequestCallback(httpPostRequestCallback)
                .build();
    }

    /** Builds sink config for the DataStream API where options are supplied only via Properties. */
    public static HttpSinkConfig fromDataStream(
            String endpointUrl,
            Properties properties,
            HttpPostRequestCallback<HttpRequest> httpPostRequestCallback) {

        validateLegacyAndNewStatusCodeOptionsAreExclusive(new Configuration(), properties);
        Configuration configuration = new Configuration();
        String requestTimeout =
                properties.getProperty(HttpConnectorConfigConstants.SINK_HTTP_TIMEOUT_SECONDS);
        if (requestTimeout != null) {
            configuration.set(SINK_REQUEST_TIMEOUT, TimeUtils.parseDuration(requestTimeout));
        }

        String writerThreadPoolSize =
                properties.getProperty(
                        HttpConnectorConfigConstants.SINK_HTTP_WRITER_THREAD_POOL_SIZE);
        if (writerThreadPoolSize != null) {
            configuration.set(SINK_WRITER_THREAD_POOL_SIZE, Integer.parseInt(writerThreadPoolSize));
        }

        String maxRetries = properties.getProperty(HttpConnectorConfigConstants.SINK_MAX_RETRIES);
        if (maxRetries != null) {
            configuration.set(SINK_MAX_RETRIES, Integer.parseInt(maxRetries));
        }

        String successCodes =
                properties.getProperty(HttpConnectorConfigConstants.SINK_SUCCESS_CODES);
        if (successCodes != null) {
            configuration.set(SINK_HTTP_SUCCESS_CODES, successCodes);
        }

        String retryCodes = properties.getProperty(HttpConnectorConfigConstants.SINK_RETRY_CODES);
        if (retryCodes != null) {
            configuration.set(SINK_HTTP_RETRY_CODES, retryCodes);
        }

        setIgnoredResponseCodes(configuration, properties);

        String retryStrategy =
                properties.getProperty(HttpConnectorConfigConstants.SINK_RETRY_STRATEGY_TYPE);
        if (retryStrategy != null) {
            configuration.set(SINK_RETRY_STRATEGY, retryStrategy);
        }

        String retryFixedDelay =
                properties.getProperty(HttpConnectorConfigConstants.SINK_RETRY_FIXED_DELAY_DELAY);
        if (retryFixedDelay != null) {
            configuration.set(
                    SINK_RETRY_FIXED_DELAY_DELAY, TimeUtils.parseDuration(retryFixedDelay));
        }

        String retryExponentialInitialBackoff =
                properties.getProperty(
                        HttpConnectorConfigConstants.SINK_RETRY_EXP_DELAY_INITIAL_BACKOFF);
        if (retryExponentialInitialBackoff != null) {
            configuration.set(
                    SINK_RETRY_EXPONENTIAL_DELAY_INITIAL_BACKOFF,
                    TimeUtils.parseDuration(retryExponentialInitialBackoff));
        }

        String retryExponentialMaxBackoff =
                properties.getProperty(
                        HttpConnectorConfigConstants.SINK_RETRY_EXP_DELAY_MAX_BACKOFF);
        if (retryExponentialMaxBackoff != null) {
            configuration.set(
                    SINK_RETRY_EXPONENTIAL_DELAY_MAX_BACKOFF,
                    TimeUtils.parseDuration(retryExponentialMaxBackoff));
        }

        String retryExponentialMultiplier =
                properties.getProperty(
                        HttpConnectorConfigConstants.SINK_RETRY_EXP_DELAY_MULTIPLIER);
        if (retryExponentialMultiplier != null) {
            configuration.set(
                    SINK_RETRY_EXPONENTIAL_DELAY_MULTIPLIER,
                    Double.parseDouble(retryExponentialMultiplier));
        }

        return HttpSinkConfig.builder()
                .url(endpointUrl)
                .properties(properties)
                .readableConfig(configuration)
                .httpPostRequestCallback(httpPostRequestCallback)
                .build();
    }

    private static void setLegacyIgnoredResponseCodesIfNeeded(
            Configuration configuration, Properties properties) {
        String ignoredResponseCodes = configuration.get(SINK_HTTP_IGNORED_RESPONSE_CODES);
        if (ignoredResponseCodes == null || ignoredResponseCodes.isBlank()) {
            String legacyIncludedCodes =
                    properties.getProperty(
                            HttpConnectorConfigConstants.HTTP_ERROR_SINK_CODE_INCLUDE_LIST);
            if (legacyIncludedCodes != null) {
                configuration.set(SINK_HTTP_IGNORED_RESPONSE_CODES, legacyIncludedCodes);
            }
        }
    }

    private static void setIgnoredResponseCodes(
            Configuration configuration, Properties properties) {
        String ignoredResponseCodes =
                properties.getProperty(HttpConnectorConfigConstants.SINK_IGNORE_RESPONSE_CODES);
        if (ignoredResponseCodes != null) {
            configuration.set(SINK_HTTP_IGNORED_RESPONSE_CODES, ignoredResponseCodes);
            return;
        }
        setLegacyIgnoredResponseCodesIfNeeded(configuration, properties);
    }

    /**
     * Legacy {@code http.sink.error.code} properties and the new {@code http.sink.success-codes} /
     * {@code http.sink.retry-codes} options cannot be set together. Legacy {@code
     * http.sink.error.code.exclude} also cannot be set together with {@code
     * http.sink.ignored-response-codes}. Defaults on the new options do not count as an explicit
     * choice.
     *
     * <p>{@code http.sink.ignored-response-codes} is detected only from {@code properties}. The
     * factory may copy legacy exclude into {@link ReadableConfig}; that mapped value is not treated
     * as user-set.
     */
    public static void validateLegacyAndNewStatusCodeOptionsAreExclusive(
            ReadableConfig readableConfig, Properties properties) {
        List<String> legacyOptions = collectLegacyErrorCodeOptions(properties);
        List<String> newOptions = collectExplicitNewStatusCodeOptions(readableConfig, properties);
        if (!legacyOptions.isEmpty() && !newOptions.isEmpty()) {
            throw new IllegalArgumentException(
                    "Cannot set legacy HTTP sink error-code properties ("
                            + String.join(", ", legacyOptions)
                            + ") together with "
                            + String.join(" and ", newOptions)
                            + ". Use either the legacy error-code properties or the new status-code"
                            + " options.");
        }
        validateLegacyExcludeAndIgnoredResponseCodesAreExclusive(properties);
    }

    /**
     * Rejects mixing legacy exclude with user-set ignored codes. Ignored is read only from {@code
     * properties} so a factory-mapped exclude value in {@link ReadableConfig} does not
     * false-trigger.
     */
    private static void validateLegacyExcludeAndIgnoredResponseCodesAreExclusive(
            Properties properties) {
        if (!hasNonBlankProperty(
                properties, HttpConnectorConfigConstants.HTTP_ERROR_SINK_CODE_INCLUDE_LIST)) {
            return;
        }
        if (!hasNonBlankProperty(
                properties, HttpConnectorConfigConstants.SINK_IGNORE_RESPONSE_CODES)) {
            return;
        }
        throw new IllegalArgumentException(
                "Cannot set legacy HTTP sink error-code property ("
                        + HttpConnectorConfigConstants.HTTP_ERROR_SINK_CODE_INCLUDE_LIST
                        + ") together with "
                        + SINK_HTTP_IGNORED_RESPONSE_CODES.key()
                        + ". Use either the legacy exclude property or "
                        + SINK_HTTP_IGNORED_RESPONSE_CODES.key()
                        + ".");
    }

    private static List<String> collectLegacyErrorCodeOptions(Properties properties) {
        List<String> legacyOptions = new ArrayList<>();
        addIfNonBlank(
                legacyOptions, properties, HttpConnectorConfigConstants.HTTP_ERROR_SINK_CODES_LIST);
        addIfNonBlank(
                legacyOptions,
                properties,
                HttpConnectorConfigConstants.HTTP_ERROR_SINK_CODE_INCLUDE_LIST);
        return legacyOptions;
    }

    private static List<String> collectExplicitNewStatusCodeOptions(
            ReadableConfig readableConfig, Properties properties) {
        List<String> newOptions = new ArrayList<>();
        if (isExplicitlySet(readableConfig, SINK_HTTP_SUCCESS_CODES)
                || hasNonBlankProperty(
                        properties, HttpConnectorConfigConstants.SINK_SUCCESS_CODES)) {
            newOptions.add(SINK_HTTP_SUCCESS_CODES.key());
        }
        if (isExplicitlySet(readableConfig, SINK_HTTP_RETRY_CODES)
                || hasNonBlankProperty(properties, HttpConnectorConfigConstants.SINK_RETRY_CODES)) {
            newOptions.add(SINK_HTTP_RETRY_CODES.key());
        }
        return newOptions.stream().distinct().collect(Collectors.toList());
    }

    private static boolean isExplicitlySet(
            ReadableConfig readableConfig, ConfigOption<String> option) {
        return readableConfig != null && readableConfig.getOptional(option).isPresent();
    }

    private static void addIfNonBlank(List<String> options, Properties properties, String key) {
        if (hasNonBlankProperty(properties, key)) {
            options.add(key);
        }
    }

    private static boolean hasNonBlankProperty(Properties properties, String key) {
        return properties != null
                && !StringUtils.isNullOrWhitespaceOnly(properties.getProperty(key));
    }
}
