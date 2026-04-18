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

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.connector.base.sink.writer.AsyncSinkWriter;
import org.apache.flink.connector.base.sink.writer.BufferedRequestState;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.connector.http.HttpSink;
import org.apache.flink.connector.http.clients.SinkHttpClient;
import org.apache.flink.connector.http.config.HttpConnectorConfigConstants;
import org.apache.flink.connector.http.utils.ThreadUtils;
import org.apache.flink.metrics.Counter;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import lombok.extern.slf4j.Slf4j;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Sink writer created by {@link HttpSink} to write to an HTTP endpoint.
 *
 * <p>More details on the internals of this sink writer may be found in {@link AsyncSinkWriter}
 * documentation.
 *
 * @param <InputT> type of the elements that should be sent through HTTP request.
 */
@Slf4j
public class HttpSinkWriter<InputT> extends AsyncSinkWriter<InputT, HttpSinkRequestEntry> {

    private static final String HTTP_SINK_WRITER_THREAD_POOL_SIZE = "4";

    private static final int DEFAULT_MAX_RETRY_TIMES = 3;

    private static final long RETRY_INITIAL_BACKOFF_MS = 1000L;

    /** Thread pool to handle HTTP response from HTTP client. */
    private final ExecutorService sinkWriterThreadPool;

    private final String endpointUrl;

    private final SinkHttpClient sinkHttpClient;

    private final Counter numRecordsSendErrorsCounter;

    private final int maxRetryTimes;

    public HttpSinkWriter(
            ElementConverter<InputT, HttpSinkRequestEntry> elementConverter,
            Sink.InitContext context,
            int maxBatchSize,
            int maxInFlightRequests,
            int maxBufferedRequests,
            long maxBatchSizeInBytes,
            long maxTimeInBufferMS,
            long maxRecordSizeInBytes,
            String endpointUrl,
            SinkHttpClient sinkHttpClient,
            Collection<BufferedRequestState<HttpSinkRequestEntry>> bufferedRequestStates,
            Properties properties) {

        super(
                elementConverter,
                context,
                maxBatchSize,
                maxInFlightRequests,
                maxBufferedRequests,
                maxBatchSizeInBytes,
                maxTimeInBufferMS,
                maxRecordSizeInBytes,
                bufferedRequestStates);
        this.endpointUrl = endpointUrl;
        this.sinkHttpClient = sinkHttpClient;

        var metrics = context.metricGroup();
        this.numRecordsSendErrorsCounter = metrics.getNumRecordsSendErrorsCounter();

        int sinkWriterThreadPoolSize =
                Integer.parseInt(
                        properties.getProperty(
                                HttpConnectorConfigConstants.SINK_HTTP_WRITER_THREAD_POOL_SIZE,
                                HTTP_SINK_WRITER_THREAD_POOL_SIZE));

        this.maxRetryTimes =
                Integer.parseInt(
                        properties.getProperty(
                                HttpConnectorConfigConstants.SINK_HTTP_RETRY_TIMES,
                                String.valueOf(DEFAULT_MAX_RETRY_TIMES)));

        this.sinkWriterThreadPool =
                Executors.newFixedThreadPool(
                        sinkWriterThreadPoolSize,
                        new ExecutorThreadFactory(
                                "http-sink-writer-worker", ThreadUtils.LOGGING_EXCEPTION_HANDLER));
    }

    @Override
    protected void submitRequestEntries(
            List<HttpSinkRequestEntry> requestEntries,
            Consumer<List<HttpSinkRequestEntry>> requestResult) {
        submitWithRetry(requestEntries, requestResult, 0);
    }

    private void submitWithRetry(
            List<HttpSinkRequestEntry> requestEntries,
            Consumer<List<HttpSinkRequestEntry>> requestResult,
            int attempt) {
        var future = sinkHttpClient.putRequests(requestEntries, endpointUrl);
        future.whenCompleteAsync(
                (response, err) -> {
                    if (err != null) {
                        // Network-level failure (e.g. IOException)
                        if (attempt < maxRetryTimes) {
                            long backoffMs = RETRY_INITIAL_BACKOFF_MS * (1L << attempt);
                            log.warn(
                                    "Http Sink failed to write {} requests due to error, "
                                            + "retrying (attempt {}/{}) after {}ms: {}",
                                    requestEntries.size(),
                                    attempt + 1,
                                    maxRetryTimes,
                                    backoffMs,
                                    err.getMessage());
                            scheduleRetry(requestEntries, requestResult, attempt, backoffMs);
                        } else {
                            int failedRequestsNumber = requestEntries.size();
                            log.error(
                                    "Http Sink fatally failed to write all {} requests"
                                            + " after {} retries",
                                    failedRequestsNumber,
                                    maxRetryTimes);
                            numRecordsSendErrorsCounter.inc(failedRequestsNumber);
                            requestResult.accept(Collections.emptyList());
                        }
                    } else if (!response.getFailedRequests().isEmpty()) {
                        // HTTP-level failure (e.g. 5xx response)
                        int failedRequestsNumber = response.getFailedRequests().size();
                        if (attempt < maxRetryTimes) {
                            long backoffMs = RETRY_INITIAL_BACKOFF_MS * (1L << attempt);
                            log.warn(
                                    "Http Sink received {} failed HTTP responses, "
                                            + "retrying (attempt {}/{}) after {}ms",
                                    failedRequestsNumber,
                                    attempt + 1,
                                    maxRetryTimes,
                                    backoffMs);
                            // Retry with the original requestEntries since HttpRequest is an
                            // internal representation and cannot be passed back to putRequests
                            scheduleRetry(requestEntries, requestResult, attempt, backoffMs);
                        } else {
                            log.error(
                                    "Http Sink failed to write {} requests after {} retries",
                                    failedRequestsNumber,
                                    maxRetryTimes);
                            numRecordsSendErrorsCounter.inc(failedRequestsNumber);
                            requestResult.accept(Collections.emptyList());
                        }
                    } else {
                        // All requests succeeded
                        requestResult.accept(Collections.emptyList());
                    }
                },
                sinkWriterThreadPool);
    }

    private void scheduleRetry(
            List<HttpSinkRequestEntry> requestEntries,
            Consumer<List<HttpSinkRequestEntry>> requestResult,
            int attempt,
            long backoffMs) {
        sinkWriterThreadPool.submit(
                () -> {
                    try {
                        TimeUnit.MILLISECONDS.sleep(backoffMs);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                    }
                    submitWithRetry(requestEntries, requestResult, attempt + 1);
                });
    }

    @Override
    protected long getSizeInBytes(HttpSinkRequestEntry s) {
        return s.getSizeInBytes();
    }

    @Override
    public void close() {
        sinkWriterThreadPool.shutdownNow();
        super.close();
    }
}
