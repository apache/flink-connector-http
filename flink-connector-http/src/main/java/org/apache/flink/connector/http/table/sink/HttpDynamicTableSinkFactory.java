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
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.base.table.AsyncDynamicTableSinkFactory;
import org.apache.flink.connector.base.table.sink.options.AsyncSinkConfigurationValidator;
import org.apache.flink.connector.http.HttpPostRequestCallbackFactory;
import org.apache.flink.connector.http.config.HttpConnectorConfigConstants;
import org.apache.flink.connector.http.config.HttpSinkConfig;
import org.apache.flink.connector.http.config.HttpSinkConfigFactory;
import org.apache.flink.connector.http.retry.RetryStrategyType;
import org.apache.flink.connector.http.sink.httpclient.HttpRequest;
import org.apache.flink.connector.http.status.HttpCodesParser;
import org.apache.flink.connector.http.status.HttpResponseChecker;
import org.apache.flink.connector.http.utils.ConfigUtils;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.util.ConfigurationException;

import java.time.Duration;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import static org.apache.flink.connector.http.table.sink.HttpDynamicSinkConnectorOptions.INSERT_METHOD;
import static org.apache.flink.connector.http.table.sink.HttpDynamicSinkConnectorOptions.REQUEST_CALLBACK_IDENTIFIER;
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
import static org.apache.flink.connector.http.table.sink.HttpDynamicSinkConnectorOptions.URL;

/** Factory for creating {@link HttpDynamicSink}. */
public class HttpDynamicTableSinkFactory extends AsyncDynamicTableSinkFactory {

    public static final String IDENTIFIER = "http-async-sink";

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        final AsyncDynamicSinkContext factoryContext = new AsyncDynamicSinkContext(this, context);

        // This is actually same as calling helper.getOptions();
        ReadableConfig tableOptions = factoryContext.getTableOptions();

        // Validate configuration
        FactoryUtil.createTableFactoryHelper(this, context)
                .validateExcept(
                        // properties coming from
                        // org.apache.flink.table.api.config.ExecutionConfigOptions
                        "table.", HttpConnectorConfigConstants.FLINK_CONNECTOR_HTTP);
        validateHttpSinkOptions(tableOptions);

        Properties asyncSinkProperties =
                new AsyncSinkConfigurationValidator(tableOptions).getValidatedConfigurations();

        // generics type erasure, so we have to do an unchecked cast
        final HttpPostRequestCallbackFactory<HttpRequest> postRequestCallbackFactory =
                FactoryUtil.discoverFactory(
                        context.getClassLoader(),
                        HttpPostRequestCallbackFactory.class, // generics type erasure
                        tableOptions.get(REQUEST_CALLBACK_IDENTIFIER));

        Properties httpConnectorProperties =
                ConfigUtils.getHttpConnectorProperties(context.getCatalogTable().getOptions());

        HttpSinkConfig sinkConfig =
                HttpSinkConfigFactory.fromTableOptions(
                        tableOptions,
                        httpConnectorProperties,
                        postRequestCallbackFactory.createHttpPostRequestCallback());

        HttpDynamicSink.HttpDynamicTableSinkBuilder builder =
                new HttpDynamicSink.HttpDynamicTableSinkBuilder()
                        .setSinkConfig(sinkConfig)
                        .setEncodingFormat(factoryContext.getEncodingFormat())
                        .setConsumedDataType(factoryContext.getPhysicalDataType());
        addAsyncOptionsToBuilder(asyncSinkProperties, builder);

        return builder.build();
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Set.of(URL, FactoryUtil.FORMAT);
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        var options = super.optionalOptions();
        options.add(INSERT_METHOD);
        options.add(SINK_REQUEST_TIMEOUT);
        options.add(SINK_WRITER_THREAD_POOL_SIZE);
        options.add(REQUEST_CALLBACK_IDENTIFIER);
        options.add(SINK_MAX_RETRIES);
        options.add(SINK_HTTP_SUCCESS_CODES);
        options.add(SINK_HTTP_RETRY_CODES);
        options.add(SINK_HTTP_IGNORED_RESPONSE_CODES);
        options.add(SINK_RETRY_STRATEGY);
        options.add(SINK_RETRY_FIXED_DELAY_DELAY);
        options.add(SINK_RETRY_EXPONENTIAL_DELAY_INITIAL_BACKOFF);
        options.add(SINK_RETRY_EXPONENTIAL_DELAY_MAX_BACKOFF);
        options.add(SINK_RETRY_EXPONENTIAL_DELAY_MULTIPLIER);
        return options;
    }

    private void validateHttpSinkOptions(ReadableConfig tableOptions)
            throws IllegalArgumentException {
        tableOptions
                .getOptional(INSERT_METHOD)
                .ifPresent(
                        insertMethod -> {
                            if (!Set.of("POST", "PUT").contains(insertMethod)) {
                                throw new IllegalArgumentException(
                                        String.format(
                                                "Invalid option '%s'. It is expected to be either 'POST' or 'PUT'.",
                                                INSERT_METHOD.key()));
                            }
                        });
        tableOptions
                .getOptional(SINK_WRITER_THREAD_POOL_SIZE)
                .ifPresent(
                        threadPoolSize -> {
                            if (threadPoolSize < 1) {
                                throw new IllegalArgumentException(
                                        String.format(
                                                "Invalid option '%s'. It must be greater than 0 but was: %s",
                                                SINK_WRITER_THREAD_POOL_SIZE.key(),
                                                threadPoolSize));
                            }
                        });
        tableOptions
                .getOptional(SINK_MAX_RETRIES)
                .ifPresent(
                        maxRetries -> {
                            if (maxRetries < 0) {
                                throw new IllegalArgumentException(
                                        String.format(
                                                "Invalid option '%s'. It must be greater than or equal to 0 but was: %s",
                                                SINK_MAX_RETRIES.key(), maxRetries));
                            }
                        });
        tableOptions
                .getOptional(SINK_RETRY_STRATEGY)
                .ifPresent(
                        retryStrategy -> {
                            try {
                                RetryStrategyType.fromCode(retryStrategy);
                            } catch (IllegalArgumentException e) {
                                throw new IllegalArgumentException(
                                        String.format(
                                                "Invalid option '%s'. Supported values are: fixed-delay, exponential-delay.",
                                                SINK_RETRY_STRATEGY.key()),
                                        e);
                            }
                        });
        tableOptions
                .getOptional(SINK_RETRY_EXPONENTIAL_DELAY_MULTIPLIER)
                .ifPresent(
                        multiplier -> {
                            if (multiplier <= 0) {
                                throw new IllegalArgumentException(
                                        String.format(
                                                "Invalid option '%s'. It must be greater than 0 but was: %s",
                                                SINK_RETRY_EXPONENTIAL_DELAY_MULTIPLIER.key(),
                                                multiplier));
                            }
                        });
        validatePositiveDuration(tableOptions, SINK_RETRY_FIXED_DELAY_DELAY);
        validatePositiveDuration(tableOptions, SINK_RETRY_EXPONENTIAL_DELAY_INITIAL_BACKOFF);
        validatePositiveDuration(tableOptions, SINK_RETRY_EXPONENTIAL_DELAY_MAX_BACKOFF);
        validateExponentialBackoffRange(tableOptions);
        validateHttpSinkStatusCodeOptions(tableOptions);
    }

    private void validatePositiveDuration(
            ReadableConfig tableOptions, ConfigOption<Duration> option) {
        Duration duration = tableOptions.get(option);
        if (duration.isZero() || duration.isNegative()) {
            throw new IllegalArgumentException(
                    String.format(
                            "Invalid option '%s'. It must be greater than 0 but was: %s",
                            option.key(), duration));
        }
    }

    private void validateExponentialBackoffRange(ReadableConfig tableOptions) {
        Duration initialBackoff = tableOptions.get(SINK_RETRY_EXPONENTIAL_DELAY_INITIAL_BACKOFF);
        Duration maxBackoff = tableOptions.get(SINK_RETRY_EXPONENTIAL_DELAY_MAX_BACKOFF);
        if (maxBackoff.compareTo(initialBackoff) < 0) {
            throw new IllegalArgumentException(
                    String.format(
                            "Invalid option '%s'. It must be greater than or equal to '%s'.",
                            SINK_RETRY_EXPONENTIAL_DELAY_MAX_BACKOFF.key(),
                            SINK_RETRY_EXPONENTIAL_DELAY_INITIAL_BACKOFF.key()));
        }
    }

    private void validateHttpSinkStatusCodeOptions(ReadableConfig tableOptions) {
        Properties httpConnectorProperties = new Properties();
        tableOptions.toMap().forEach(httpConnectorProperties::setProperty);
        HttpSinkConfigFactory.validateLegacyAndNewStatusCodeOptionsAreExclusive(
                tableOptions, httpConnectorProperties);
        try {
            var ignoredCodes =
                    HttpCodesParser.parse(tableOptions.get(SINK_HTTP_IGNORED_RESPONSE_CODES));
            var successCodes = new HashSet<Integer>();
            successCodes.addAll(HttpCodesParser.parse(tableOptions.get(SINK_HTTP_SUCCESS_CODES)));
            successCodes.addAll(ignoredCodes);
            var retryCodes =
                    new HashSet<>(HttpCodesParser.parse(tableOptions.get(SINK_HTTP_RETRY_CODES)));
            retryCodes.removeAll(ignoredCodes);
            new HttpResponseChecker(successCodes, retryCodes);
        } catch (ConfigurationException e) {
            throw new IllegalArgumentException("Invalid HTTP sink status-code configuration.", e);
        }
    }
}
