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

import org.apache.flink.api.connector.sink2.StatefulSinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.connector.base.sink.AsyncSinkBase;
import org.apache.flink.connector.base.sink.writer.BufferedRequestState;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.connector.http.HttpSink;
import org.apache.flink.connector.http.HttpSinkBuilder;
import org.apache.flink.connector.http.SchemaLifecycleAwareElementConverter;
import org.apache.flink.connector.http.clients.SinkHttpClient;
import org.apache.flink.connector.http.clients.SinkHttpClientBuilder;
import org.apache.flink.connector.http.clients.SinkHttpClientContext;
import org.apache.flink.connector.http.config.HttpSinkConfig;
import org.apache.flink.connector.http.preprocessor.HeaderPreprocessor;
import org.apache.flink.connector.http.sink.httpclient.HttpRequest;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

/**
 * An internal implementation of HTTP Sink that performs async requests against a specified HTTP
 * endpoint using the buffering protocol specified in {@link AsyncSinkBase}.
 *
 * <p>API of this class can change without any concerns as long as it does not have any influence on
 * methods defined in {@link HttpSink} and {@link HttpSinkBuilder} classes.
 *
 * @param <InputT> type of the elements that should be sent through HTTP request.
 */
public class HttpSinkInternal<InputT> extends AsyncSinkBase<InputT, HttpSinkRequestEntry> {

    // having Builder instead of an instance of `SinkHttpClient`
    // makes it possible to serialize `HttpSink`
    private final SinkHttpClientBuilder sinkHttpClientBuilder;

    private final HeaderPreprocessor headerPreprocessor;

    private final HttpSinkConfig sinkConfig;

    protected HttpSinkInternal(
            ElementConverter<InputT, HttpSinkRequestEntry> elementConverter,
            int maxBatchSize,
            int maxInFlightRequests,
            int maxBufferedRequests,
            long maxBatchSizeInBytes,
            long maxTimeInBufferMS,
            long maxRecordSizeInBytes,
            HttpSinkConfig sinkConfig,
            HeaderPreprocessor headerPreprocessor,
            SinkHttpClientBuilder sinkHttpClientBuilder) {

        super(
                elementConverter,
                maxBatchSize,
                maxInFlightRequests,
                maxBufferedRequests,
                maxBatchSizeInBytes,
                maxTimeInBufferMS,
                maxRecordSizeInBytes);

        Preconditions.checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(sinkConfig.getUrl()),
                "The endpoint URL must be set when initializing HTTP Sink.");
        Preconditions.checkNotNull(
                sinkConfig.getHttpPostRequestCallback(),
                "Post request callback must be set when initializing HTTP Sink.");
        this.sinkConfig = sinkConfig;
        this.headerPreprocessor =
                Preconditions.checkNotNull(
                        headerPreprocessor,
                        "Header Preprocessor must be set when initializing HTTP Sink.");
        this.sinkHttpClientBuilder =
                Preconditions.checkNotNull(
                        sinkHttpClientBuilder,
                        "The HTTP client builder must not be null when initializing HTTP Sink.");
    }

    @Override
    public StatefulSinkWriter<InputT, BufferedRequestState<HttpSinkRequestEntry>> createWriter(
            WriterInitContext context) throws IOException {

        ElementConverter<InputT, HttpSinkRequestEntry> elementConverter = getElementConverter();
        if (elementConverter instanceof SchemaLifecycleAwareElementConverter) {
            // This cast is needed for Flink 1.15.3 build
            ((SchemaLifecycleAwareElementConverter<?, ?>) elementConverter).open(context);
        }

        return new HttpSinkWriter<>(
                elementConverter,
                context,
                getMaxBatchSize(),
                getMaxInFlightRequests(),
                getMaxBufferedRequests(),
                getMaxBatchSizeInBytes(),
                getMaxTimeInBufferMS(),
                getMaxRecordSizeInBytes(),
                sinkConfig.getUrl(),
                buildSinkHttpClient(),
                Collections.emptyList(),
                sinkConfig);
    }

    @Override
    public StatefulSinkWriter<InputT, BufferedRequestState<HttpSinkRequestEntry>> restoreWriter(
            WriterInitContext context,
            Collection<BufferedRequestState<HttpSinkRequestEntry>> recoveredState)
            throws IOException {

        return new HttpSinkWriter<>(
                getElementConverter(),
                context,
                getMaxBatchSize(),
                getMaxInFlightRequests(),
                getMaxBufferedRequests(),
                getMaxBatchSizeInBytes(),
                getMaxTimeInBufferMS(),
                getMaxRecordSizeInBytes(),
                sinkConfig.getUrl(),
                buildSinkHttpClient(),
                recoveredState,
                sinkConfig);
    }

    private SinkHttpClient buildSinkHttpClient() {
        return sinkHttpClientBuilder.build(
                new DefaultSinkHttpClientContext(
                        sinkConfig, headerPreprocessor, getMaxBatchSize()));
    }

    @Override
    public SimpleVersionedSerializer<BufferedRequestState<HttpSinkRequestEntry>>
            getWriterStateSerializer() {
        return new HttpSinkWriterStateSerializer();
    }

    private static class DefaultSinkHttpClientContext implements SinkHttpClientContext {

        private final HttpSinkConfig sinkConfig;
        private final HeaderPreprocessor headerPreprocessor;
        private final int defaultBatchSize;

        private DefaultSinkHttpClientContext(
                HttpSinkConfig sinkConfig,
                HeaderPreprocessor headerPreprocessor,
                int defaultBatchSize) {
            this.sinkConfig = sinkConfig;
            this.headerPreprocessor = headerPreprocessor;
            this.defaultBatchSize = defaultBatchSize;
        }

        @Override
        public HttpSinkConfig getSinkConfig() {
            return sinkConfig;
        }

        @Override
        public org.apache.flink.connector.http.HttpPostRequestCallback<HttpRequest>
                getHttpPostRequestCallback() {
            return sinkConfig.getHttpPostRequestCallback();
        }

        @Override
        public HeaderPreprocessor getHeaderPreprocessor() {
            return headerPreprocessor;
        }

        @Override
        public int getDefaultBatchSize() {
            return defaultBatchSize;
        }
    }
}
