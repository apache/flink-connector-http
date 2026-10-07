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

import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.connector.base.sink.writer.AsyncSinkWriter;
import org.apache.flink.connector.base.sink.writer.BufferedRequestState;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.connector.base.sink.writer.ResultHandler;
import org.apache.flink.connector.base.sink.writer.config.AsyncSinkWriterConfiguration;
import org.apache.flink.connector.http.HttpSink;
import org.apache.flink.connector.http.clients.SinkHttpClient;
import org.apache.flink.connector.http.config.HttpSinkConfig;
import org.apache.flink.connector.http.utils.ThreadUtils;
import org.apache.flink.metrics.Counter;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import lombok.extern.slf4j.Slf4j;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Sink writer created by {@link HttpSink} to write to an HTTP endpoint.
 *
 * <p>More details on the internals of this sink writer may be found in {@link AsyncSinkWriter}
 * documentation.
 *
 * <p>Note: This class extends {@link AsyncSinkWriter} which has deprecated constructors in Flink
 * 2.x. The deprecation originates from Flink's base connector framework and would require
 * significant refactoring to address. This HTTP Connector issue is tracked by Jira
 * https://issues.apache.org/jira/browse/FLINK-39536.
 *
 * @param <InputT> type of the elements that should be sent through HTTP request.
 */
@Slf4j
@SuppressWarnings("deprecation") // AsyncSinkWriter constructor is deprecated in Flink 2.x
public class HttpSinkWriter<InputT> extends AsyncSinkWriter<InputT, HttpSinkRequestEntry> {

    /** Thread pool to handle HTTP response from HTTP client. */
    private final ExecutorService sinkWriterThreadPool;

    private final String endpointUrl;

    private final SinkHttpClient sinkHttpClient;

    private final Counter numRecordsSendErrorsCounter;

    public HttpSinkWriter(
            ElementConverter<InputT, HttpSinkRequestEntry> elementConverter,
            WriterInitContext context,
            int maxBatchSize,
            int maxInFlightRequests,
            int maxBufferedRequests,
            long maxBatchSizeInBytes,
            long maxTimeInBufferMS,
            long maxRecordSizeInBytes,
            String endpointUrl,
            SinkHttpClient sinkHttpClient,
            Collection<BufferedRequestState<HttpSinkRequestEntry>> bufferedRequestStates,
            HttpSinkConfig sinkConfig) {

        super(
                elementConverter,
                context,
                AsyncSinkWriterConfiguration.builder()
                        .setMaxBatchSize(maxBatchSize)
                        .setMaxBatchSizeInBytes(maxBatchSizeInBytes)
                        .setMaxInFlightRequests(maxInFlightRequests)
                        .setMaxBufferedRequests(maxBufferedRequests)
                        .setMaxTimeInBufferMS(maxTimeInBufferMS)
                        .setMaxRecordSizeInBytes(maxRecordSizeInBytes)
                        .build(),
                bufferedRequestStates);
        this.endpointUrl = endpointUrl;
        this.sinkHttpClient = sinkHttpClient;

        var metrics = context.metricGroup();
        this.numRecordsSendErrorsCounter = metrics.getNumRecordsSendErrorsCounter();

        int sinkWriterThreadPoolSize = sinkConfig.getWriterThreadPoolSize();

        this.sinkWriterThreadPool =
                Executors.newFixedThreadPool(
                        sinkWriterThreadPoolSize,
                        new ExecutorThreadFactory(
                                "http-sink-writer-worker", ThreadUtils.LOGGING_EXCEPTION_HANDLER));
    }

    @Override
    protected void submitRequestEntries(
            List<HttpSinkRequestEntry> requestEntries,
            ResultHandler<HttpSinkRequestEntry> resultHandler) {
        var future = sinkHttpClient.putRequests(requestEntries, endpointUrl);
        future.whenCompleteAsync(
                (response, err) -> {
                    if (err != null) {
                        int failedRequestsNumber = requestEntries.size();
                        log.error(
                                "Http Sink fatally failed to write all {} requests",
                                failedRequestsNumber);
                        numRecordsSendErrorsCounter.inc(failedRequestsNumber);

                        resultHandler.completeExceptionally(
                                new RuntimeException(
                                        "HTTP sink request failed before receiving a response for "
                                                + failedRequestsNumber
                                                + " request(s).",
                                        err));
                    } else if (!response.getFatalFailedRequests().isEmpty()) {
                        int failedRequestsNumber = response.getFatalFailedRequests().size();
                        log.error("Http Sink failed to write {} requests", failedRequestsNumber);
                        numRecordsSendErrorsCounter.inc(failedRequestsNumber);
                        resultHandler.completeExceptionally(
                                new RuntimeException(
                                        "HTTP sink received fatal response status for "
                                                + failedRequestsNumber
                                                + " request(s)."));
                    } else if (!response.getRetriableFailedRequests().isEmpty()) {
                        int failedRequestsNumber = response.getRetriableFailedRequests().size();
                        log.error(
                                "Http Sink exhausted client-level retries for {} retryable requests",
                                failedRequestsNumber);
                        numRecordsSendErrorsCounter.inc(failedRequestsNumber);
                        resultHandler.completeExceptionally(
                                new RuntimeException(
                                        "HTTP sink exhausted retries for "
                                                + failedRequestsNumber
                                                + " request(s)."));
                    } else {
                        resultHandler.complete();
                    }
                },
                sinkWriterThreadPool);
    }

    @Override
    protected long getSizeInBytes(HttpSinkRequestEntry s) {
        return s.getSizeInBytes();
    }

    @Override
    public void close() {
        sinkHttpClient.close();
        sinkWriterThreadPool.shutdownNow();
        super.close();
    }
}
