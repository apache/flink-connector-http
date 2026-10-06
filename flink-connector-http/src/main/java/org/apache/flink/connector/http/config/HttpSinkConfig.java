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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.http.HttpPostRequestCallback;
import org.apache.flink.connector.http.preprocessor.HeaderPreprocessor;
import org.apache.flink.connector.http.sink.httpclient.HttpRequest;
import org.apache.flink.connector.http.utils.HttpHeaderUtils;

import lombok.Builder;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.io.Serializable;
import java.time.Duration;
import java.util.Map;
import java.util.Properties;

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

/**
 * Unified HTTP sink configuration for table and DataStream API.
 *
 * <p>Typed user-facing options are read from {@link #readableConfig}. Open-ended keys such as
 * headers, TLS paths, legacy error-code lists, and request-mode remain in {@link #properties}.
 * Prefer typed getters (and {@link #prepareHeaderMap(HeaderPreprocessor)}) over reading properties
 * directly when a dedicated accessor exists.
 */
@Builder
@Getter
@PublicEvolving
@RequiredArgsConstructor
public class HttpSinkConfig implements Serializable {

    private final String url;

    @Builder.Default private final Properties properties = new Properties();

    // Use Configuration instead of ReadableConfig because Configuration is Serializable
    @Builder.Default private final Configuration readableConfig = new Configuration();

    private final HttpPostRequestCallback<HttpRequest> httpPostRequestCallback;

    /**
     * Gets the readable config. Returns the Configuration which implements ReadableConfig. This
     * method maintains API compatibility while using the serializable Configuration type.
     */
    public ReadableConfig getReadableConfig() {
        return readableConfig;
    }

    /**
     * Builds the sink HTTP header map from {@code http.sink.header.*} properties after
     * preprocessing.
     */
    public Map<String, String> prepareHeaderMap(HeaderPreprocessor headerPreprocessor) {
        return HttpHeaderUtils.prepareHeaderMap(
                HttpConnectorConfigConstants.SINK_HEADER_PREFIX, properties, headerPreprocessor);
    }

    /** Returns the configured sink request mode property, or {@code null} if unset. */
    public String getRequestMode() {
        return properties.getProperty(HttpConnectorConfigConstants.SINK_HTTP_REQUEST_MODE);
    }

    public Duration getRequestTimeout() {
        return readableConfig.get(SINK_REQUEST_TIMEOUT);
    }

    public int getWriterThreadPoolSize() {
        return readableConfig.get(SINK_WRITER_THREAD_POOL_SIZE);
    }

    public int getMaxRetries() {
        return readableConfig.get(SINK_MAX_RETRIES);
    }

    public String getSuccessCodes() {
        return readableConfig.get(SINK_HTTP_SUCCESS_CODES);
    }

    public String getRetryCodes() {
        return readableConfig.get(SINK_HTTP_RETRY_CODES);
    }

    public String getIgnoredResponseCodes() {
        return readableConfig.get(SINK_HTTP_IGNORED_RESPONSE_CODES);
    }

    public String getRetryStrategy() {
        return readableConfig.get(SINK_RETRY_STRATEGY);
    }

    public Duration getRetryFixedDelay() {
        return readableConfig.get(SINK_RETRY_FIXED_DELAY_DELAY);
    }

    public Duration getRetryExponentialDelayInitialBackoff() {
        return readableConfig.get(SINK_RETRY_EXPONENTIAL_DELAY_INITIAL_BACKOFF);
    }

    public Duration getRetryExponentialDelayMaxBackoff() {
        return readableConfig.get(SINK_RETRY_EXPONENTIAL_DELAY_MAX_BACKOFF);
    }

    public double getRetryExponentialDelayMultiplier() {
        return readableConfig.get(SINK_RETRY_EXPONENTIAL_DELAY_MULTIPLIER);
    }

    @PublicEvolving
    public static class HttpSinkConfigBuilder {}
}
