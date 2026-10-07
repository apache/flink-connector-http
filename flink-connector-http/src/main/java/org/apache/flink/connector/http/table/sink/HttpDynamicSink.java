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

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.base.table.sink.AsyncDynamicTableSink;
import org.apache.flink.connector.base.table.sink.AsyncDynamicTableSinkBuilder;
import org.apache.flink.connector.http.HttpSink;
import org.apache.flink.connector.http.HttpSinkBuilder;
import org.apache.flink.connector.http.config.HttpSinkConfig;
import org.apache.flink.connector.http.sink.HttpSinkRequestEntry;
import org.apache.flink.connector.http.sink.httpclient.JavaNetSinkHttpClient;
import org.apache.flink.connector.http.table.SerializationSchemaElementConverter;
import org.apache.flink.connector.http.utils.HttpHeaderUtils;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;

import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nullable;

import static org.apache.flink.connector.http.table.sink.HttpDynamicSinkConnectorOptions.INSERT_METHOD;

/**
 * A dynamic HTTP Sink based on {@link AsyncDynamicTableSink} that adds Table API support for {@link
 * HttpSink}.
 */
@Slf4j
@EqualsAndHashCode(callSuper = true)
public class HttpDynamicSink extends AsyncDynamicTableSink<HttpSinkRequestEntry> {

    private final DataType consumedDataType;

    private final EncodingFormat<SerializationSchema<RowData>> encodingFormat;

    private final HttpSinkConfig sinkConfig;

    protected HttpDynamicSink(
            @Nullable Integer maxBatchSize,
            @Nullable Integer maxInFlightRequests,
            @Nullable Integer maxBufferedRequests,
            @Nullable Long maxBufferSizeInBytes,
            @Nullable Long maxTimeInBufferMS,
            DataType consumedDataType,
            EncodingFormat<SerializationSchema<RowData>> encodingFormat,
            HttpSinkConfig sinkConfig) {
        super(
                maxBatchSize,
                maxInFlightRequests,
                maxBufferedRequests,
                maxBufferSizeInBytes,
                maxTimeInBufferMS);
        this.consumedDataType =
                Preconditions.checkNotNull(consumedDataType, "Consumed data type must not be null");
        this.encodingFormat =
                Preconditions.checkNotNull(encodingFormat, "Encoding format must not be null");
        this.sinkConfig = Preconditions.checkNotNull(sinkConfig, "Sink config must not be null");
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return encodingFormat.getChangelogMode();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        SerializationSchema<RowData> serializationSchema =
                encodingFormat.createRuntimeEncoder(context, consumedDataType);

        var insertMethod = sinkConfig.getReadableConfig().get(INSERT_METHOD);

        HttpSinkBuilder<RowData> builder =
                HttpSink.<RowData>builder()
                        .setEndpointUrl(sinkConfig.getUrl())
                        .setSinkHttpClientBuilder(JavaNetSinkHttpClient.builder())
                        .setHttpPostRequestCallback(sinkConfig.getHttpPostRequestCallback())
                        // In future header preprocessor could be set via custom factory
                        .setHttpHeaderPreprocessor(
                                HttpHeaderUtils.createBasicAuthorizationHeaderPreprocessor())
                        .setElementConverter(
                                new SerializationSchemaElementConverter(
                                        insertMethod, serializationSchema))
                        .setHttpSinkConfig(sinkConfig);
        addAsyncOptionsToSinkBuilder(builder);

        return SinkV2Provider.of(builder.build());
    }

    @Override
    public DynamicTableSink copy() {
        return new HttpDynamicSink(
                maxBatchSize,
                maxInFlightRequests,
                maxBufferedRequests,
                maxBufferSizeInBytes,
                maxTimeInBufferMS,
                consumedDataType,
                encodingFormat,
                sinkConfig);
    }

    @Override
    public String asSummaryString() {
        return "HttpSink";
    }

    /** Builder to construct {@link HttpDynamicSink}. */
    public static class HttpDynamicTableSinkBuilder
            extends AsyncDynamicTableSinkBuilder<
                    HttpSinkRequestEntry, HttpDynamicTableSinkBuilder> {

        private HttpSinkConfig sinkConfig;

        private DataType consumedDataType;

        private EncodingFormat<SerializationSchema<RowData>> encodingFormat;

        /**
         * @param sinkConfig unified HTTP sink configuration
         * @return {@link HttpDynamicTableSinkBuilder} itself
         */
        public HttpDynamicTableSinkBuilder setSinkConfig(HttpSinkConfig sinkConfig) {
            this.sinkConfig = sinkConfig;
            return this;
        }

        /**
         * @param encodingFormat the format for encoding records
         * @return {@link HttpDynamicTableSinkBuilder} itself
         */
        public HttpDynamicTableSinkBuilder setEncodingFormat(
                EncodingFormat<SerializationSchema<RowData>> encodingFormat) {
            this.encodingFormat = encodingFormat;
            return this;
        }

        /**
         * @param consumedDataType the consumed data type of the table
         * @return {@link HttpDynamicTableSinkBuilder} itself
         */
        public HttpDynamicTableSinkBuilder setConsumedDataType(DataType consumedDataType) {
            this.consumedDataType = consumedDataType;
            return this;
        }

        @Override
        public HttpDynamicSink build() {
            return new HttpDynamicSink(
                    getMaxBatchSize(),
                    getMaxInFlightRequests(),
                    getMaxBufferedRequests(),
                    getMaxBufferSizeInBytes(),
                    getMaxTimeInBufferMS(),
                    consumedDataType,
                    encodingFormat,
                    sinkConfig);
        }
    }
}
