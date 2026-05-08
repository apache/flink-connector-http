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
 * <p>The writer's retry policy mirrors the lookup source:
 *
 * <ul>
 *   <li>{@code http.sink.retry.times} — maximum number of retry attempts.
 *   <li>{@code http.sink.retry-strategy.type} — {@code fixed-delay} or {@code exponential-delay}.
 *   <li>{@code http.sink.retry-strategy.fixed-delay.delay} — fixed interval between retries.
 *   <li>{@code http.sink.retry-strategy.exponential-delay.initial-backoff / max-backoff /
 *       backoff-multiplier} — exponential backoff parameters.
 *   <li>{@code http.sink.retry-codes} — HTTP status codes that should trigger a retry (default
 *       {@code 500,503,504}).
 *   <li>{@code http.sink.success-codes} — HTTP status codes that should be treated as success
 *       (default {@code 2XX}).
 * </ul>
 *
 * @param <InputT> type of the elements that should be sent through HTTP request.
 */
@Slf4j
public class HttpSinkWriter<InputT> extends AsyncSinkWriter<InputT, HttpSinkRequestEntry> {

    private static final String HTTP_SINK_WRITER_THREAD_POOL_SIZE = "4";

    /** Thread pool to handle HTTP response from HTTP client. */
    private final ExecutorService sinkWriterThreadPool;

    private final String endpointUrl;

    private final SinkHttpClient sinkHttpClient;

    private final Counter numRecordsSendErrorsCounter;

    private final SinkRetryConfig retryConfig;

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

        this.retryConfig = SinkRetryConfig.fromProperties(properties);

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
                        // Network-level failure (e.g. IOException).
                        handleRetry(requestEntries, requestResult, attempt, err.getMessage());
                        return;
                    }

                    List<org.apache.flink.connector.http.sink.httpclient.HttpRequest> retryable =
                            response.getRetryableFailedRequests();
                    List<org.apache.flink.connector.http.sink.httpclient.HttpRequest> fatal =
                            response.getFatalFailedRequests();

                    if (!fatal.isEmpty()) {
                        // Non-retryable errors: record them and do not replay.
                        log.error(
                                "Http Sink received {} fatal (non-retryable) HTTP responses,"
                                        + " skipping retry",
                                fatal.size());
                        numRecordsSendErrorsCounter.inc(fatal.size());
                    }

                    if (retryable.isEmpty()) {
                        // Either everything succeeded or only fatal failures were returned.
                        requestResult.accept(Collections.emptyList());
                        return;
                    }

                    // We have retryable failures — retry the original requestEntries because
                    // HttpRequest is an internal representation and cannot be passed back to
                    // putRequests(). Submitting the full batch keeps the behaviour consistent
                    // with the network-level retry path below.
                    handleRetry(
                            requestEntries,
                            requestResult,
                            attempt,
                            retryable.size() + " retryable HTTP failures");
                },
                sinkWriterThreadPool);
    }

    private void handleRetry(
            List<HttpSinkRequestEntry> requestEntries,
            Consumer<List<HttpSinkRequestEntry>> requestResult,
            int attempt,
            String reason) {
        int maxRetries = retryConfig.getMaxRetries();
        if (attempt < maxRetries) {
            long backoffMs = retryConfig.backoffMillis(attempt);
            log.warn(
                    "Http Sink failed to write {} requests ({}), retrying (attempt {}/{}) after"
                            + " {}ms",
                    requestEntries.size(),
                    reason,
                    attempt + 1,
                    maxRetries,
                    backoffMs);
            scheduleRetry(requestEntries, requestResult, attempt, backoffMs);
        } else {
            int failedRequestsNumber = requestEntries.size();
            log.error(
                    "Http Sink fatally failed to write all {} requests after {} retries ({})",
                    failedRequestsNumber,
                    maxRetries,
                    reason);
            numRecordsSendErrorsCounter.inc(failedRequestsNumber);
            requestResult.accept(Collections.emptyList());
        }
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
