/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.http.sink;

import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.connector.base.sink.writer.BufferedRequestState;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.connector.base.sink.writer.ResultHandler;
import org.apache.flink.connector.http.clients.SinkHttpClient;
import org.apache.flink.connector.http.clients.SinkHttpClientResponses;
import org.apache.flink.connector.http.clients.SinkHttpClientStatistics;
import org.apache.flink.connector.http.config.HttpSinkConfigFactory;
import org.apache.flink.connector.http.table.sink.Slf4jHttpPostRequestCallback;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.OperatorIOMetricGroup;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Test for {@link HttpSinkWriter }. */
@ExtendWith(MockitoExtension.class)
class HttpSinkWriterTest {

    private HttpSinkWriter<String> httpSinkWriter;

    @Mock private ElementConverter<String, HttpSinkRequestEntry> elementConverter;

    @Mock private WriterInitContext context;

    @Mock private SinkHttpClient httpClient;

    // To work with Flink 1.15 and Flink 1.16
    @Mock(lenient = true)
    private SinkWriterMetricGroup metricGroup;

    @Mock private OperatorIOMetricGroup operatorIOMetricGroup;

    @Mock private Counter errorCounter;

    @Mock private MetricGroup httpSinkMetricGroup;

    @Mock private MetricGroup statusCodeMetricGroup;

    @Mock private Counter httpRequestsCounter;

    @Mock private Histogram requestLatencyHistogram;

    @Mock private Counter retryableResponseFailuresCounter;

    @Mock private Counter requestExceptionsCounter;

    @Mock private Counter retryAttemptsCounter;

    @Mock private Counter retryExhaustedCounter;

    @Mock private Counter fatalResponseFailuresCounter;

    @Mock private Counter ignoredResponsesCounter;

    @Mock private Counter status200Counter;

    @Mock private Counter status400Counter;

    @Mock private Counter status404Counter;

    @Mock private Counter status500Counter;

    @Mock private Counter status503Counter;

    @BeforeEach
    public void setUp() {
        when(metricGroup.getNumRecordsSendErrorsCounter()).thenReturn(errorCounter);
        when(metricGroup.getIOMetricGroup()).thenReturn(operatorIOMetricGroup);
        when(metricGroup.addGroup(HttpSinkWriter.METRIC_GROUP)).thenReturn(httpSinkMetricGroup);
        when(httpSinkMetricGroup.counter("numHttpRequests")).thenReturn(httpRequestsCounter);
        when(httpSinkMetricGroup.histogram(eq("requestLatencyMs"), any()))
                .thenReturn(requestLatencyHistogram);
        when(httpSinkMetricGroup.counter("numRetryableResponseFailures"))
                .thenReturn(retryableResponseFailuresCounter);
        when(httpSinkMetricGroup.counter("numRequestExceptions"))
                .thenReturn(requestExceptionsCounter);
        when(httpSinkMetricGroup.counter("numRetryAttempts")).thenReturn(retryAttemptsCounter);
        when(httpSinkMetricGroup.counter("numRetryExhausted")).thenReturn(retryExhaustedCounter);
        when(httpSinkMetricGroup.counter("numFatalResponseFailures"))
                .thenReturn(fatalResponseFailuresCounter);
        when(httpSinkMetricGroup.counter("numIgnoredResponses"))
                .thenReturn(ignoredResponsesCounter);
        when(httpSinkMetricGroup.addGroup(HttpSinkWriter.STATUS_CODE_GROUP))
                .thenReturn(statusCodeMetricGroup);
        lenient().when(statusCodeMetricGroup.counter("200")).thenReturn(status200Counter);
        lenient().when(statusCodeMetricGroup.counter("400")).thenReturn(status400Counter);
        lenient().when(statusCodeMetricGroup.counter("404")).thenReturn(status404Counter);
        lenient().when(statusCodeMetricGroup.counter("500")).thenReturn(status500Counter);
        lenient().when(statusCodeMetricGroup.counter("503")).thenReturn(status503Counter);
        when(context.metricGroup()).thenReturn(metricGroup);

        Collection<BufferedRequestState<HttpSinkRequestEntry>> stateBuffer = new ArrayList<>();

        this.httpSinkWriter = createWriter(stateBuffer);
    }

    @Test
    public void testTransportErrorFailsRequest() throws InterruptedException {

        CompletableFuture<SinkHttpClientResponses> future = new CompletableFuture<>();
        future.completeExceptionally(new Exception("Test Exception"));

        when(httpClient.putRequests(anyList(), anyString())).thenReturn(future);

        HttpSinkRequestEntry request = new HttpSinkRequestEntry("PUT", "hello".getBytes());
        RecordingResultHandler resultHandler = new RecordingResultHandler();

        List<HttpSinkRequestEntry> requestEntries = Collections.singletonList(request);
        this.httpSinkWriter.submitRequestEntries(requestEntries, resultHandler);

        assertThat(resultHandler.await()).isTrue();
        assertThat(resultHandler.getRetriedEntries()).isEmpty();
        assertThat(resultHandler.getFailure()).isInstanceOf(RuntimeException.class);
        assertThat(resultHandler.getFailure()).hasMessageContaining("failed before receiving");
        verify(errorCounter).inc(requestEntries.size());
        verify(requestExceptionsCounter).inc(requestEntries.size());
    }

    @Test
    public void testRetryableResponseFailsAfterClientRetriesAreExhausted()
            throws InterruptedException {
        HttpSinkRequestEntry request = new HttpSinkRequestEntry("PUT", "hello".getBytes());
        when(httpClient.putRequests(anyList(), anyString()))
                .thenReturn(
                        CompletableFuture.completedFuture(
                                new SinkHttpClientResponses(
                                        Collections.emptyList(),
                                        Collections.singletonList(request),
                                        Collections.emptyList())));

        RecordingResultHandler resultHandler = new RecordingResultHandler();
        this.httpSinkWriter.submitRequestEntries(Collections.singletonList(request), resultHandler);

        assertThat(resultHandler.await()).isTrue();
        assertThat(resultHandler.getRetriedEntries()).isEmpty();
        assertThat(resultHandler.getFailure()).isInstanceOf(RuntimeException.class);
        assertThat(resultHandler.getFailure()).hasMessageContaining("exhausted retries");
        verify(errorCounter).inc(1);
    }

    @Test
    public void testRetryExhaustionFailsRequest() throws InterruptedException {
        HttpSinkRequestEntry request = new HttpSinkRequestEntry("PUT", "hello".getBytes());
        when(httpClient.putRequests(anyList(), anyString()))
                .thenReturn(
                        CompletableFuture.completedFuture(
                                new SinkHttpClientResponses(
                                        Collections.emptyList(),
                                        Collections.singletonList(request),
                                        Collections.emptyList())));

        RecordingResultHandler resultHandler = new RecordingResultHandler();
        this.httpSinkWriter.submitRequestEntries(Collections.singletonList(request), resultHandler);

        assertThat(resultHandler.await()).isTrue();
        assertThat(resultHandler.getFailure()).isInstanceOf(RuntimeException.class);
        assertThat(resultHandler.getFailure()).hasMessageContaining("exhausted retries");
        verify(errorCounter).inc(1);
    }

    @Test
    public void testFatalResponseFailsRequest() throws InterruptedException {
        HttpSinkRequestEntry request = new HttpSinkRequestEntry("PUT", "hello".getBytes());
        when(httpClient.putRequests(anyList(), anyString()))
                .thenReturn(
                        CompletableFuture.completedFuture(
                                new SinkHttpClientResponses(
                                        Collections.emptyList(),
                                        Collections.emptyList(),
                                        Collections.singletonList(request))));

        RecordingResultHandler resultHandler = new RecordingResultHandler();
        this.httpSinkWriter.submitRequestEntries(Collections.singletonList(request), resultHandler);

        assertThat(resultHandler.await()).isTrue();
        assertThat(resultHandler.getFailure()).isInstanceOf(RuntimeException.class);
        assertThat(resultHandler.getFailure()).hasMessageContaining("fatal response status");
        verify(errorCounter).inc(1);
    }

    @Test
    public void testRecordsClientStatisticsAsMetrics() throws InterruptedException {
        HttpSinkRequestEntry request = new HttpSinkRequestEntry("PUT", "hello".getBytes());
        SinkHttpClientStatistics statistics =
                new SinkHttpClientStatistics(2, List.of(42L, 7L), Map.of(503, 1, 200, 1), 1, 0, 1);
        when(httpClient.putRequests(anyList(), anyString()))
                .thenReturn(
                        CompletableFuture.completedFuture(
                                new SinkHttpClientResponses(
                                        Collections.singletonList(request),
                                        Collections.emptyList(),
                                        Collections.emptyList(),
                                        Collections.emptyList(),
                                        statistics)));

        RecordingResultHandler resultHandler = new RecordingResultHandler();
        this.httpSinkWriter.submitRequestEntries(Collections.singletonList(request), resultHandler);

        assertThat(resultHandler.await()).isTrue();
        assertThat(resultHandler.getFailure()).isNull();
        verify(httpRequestsCounter).inc(2);
        verify(requestLatencyHistogram).update(42L);
        verify(requestLatencyHistogram).update(7L);
        verify(status503Counter).inc(1);
        verify(status200Counter).inc(1);
        verify(retryableResponseFailuresCounter).inc(1);
        verify(retryAttemptsCounter).inc(1);
        verify(retryExhaustedCounter, never()).inc(anyLong());
        verify(errorCounter, never()).inc(anyLong());
    }

    @Test
    public void testFatalResponseRecordsMetrics() throws InterruptedException {
        HttpSinkRequestEntry request = new HttpSinkRequestEntry("PUT", "hello".getBytes());
        when(httpClient.putRequests(anyList(), anyString()))
                .thenReturn(
                        CompletableFuture.completedFuture(
                                new SinkHttpClientResponses(
                                        Collections.emptyList(),
                                        Collections.emptyList(),
                                        Collections.singletonList(request),
                                        Collections.emptyList(),
                                        statistics(1, Map.of(400, 1), 0, 0, 0))));

        RecordingResultHandler resultHandler = new RecordingResultHandler();
        this.httpSinkWriter.submitRequestEntries(Collections.singletonList(request), resultHandler);

        assertThat(resultHandler.await()).isTrue();
        assertThat(resultHandler.getFailure()).hasMessageContaining("fatal response status");
        verify(fatalResponseFailuresCounter).inc(1);
        verify(status400Counter).inc(1);
        verify(errorCounter).inc(1);
    }

    @Test
    public void testIgnoredResponseCompletesAndRecordsMetrics() throws InterruptedException {
        HttpSinkRequestEntry request = new HttpSinkRequestEntry("PUT", "hello".getBytes());
        when(httpClient.putRequests(anyList(), anyString()))
                .thenReturn(
                        CompletableFuture.completedFuture(
                                new SinkHttpClientResponses(
                                        Collections.emptyList(),
                                        Collections.emptyList(),
                                        Collections.emptyList(),
                                        Collections.singletonList(request),
                                        statistics(1, Map.of(404, 1), 0, 0, 0))));

        RecordingResultHandler resultHandler = new RecordingResultHandler();
        this.httpSinkWriter.submitRequestEntries(Collections.singletonList(request), resultHandler);

        assertThat(resultHandler.await()).isTrue();
        assertThat(resultHandler.getFailure()).isNull();
        assertThat(resultHandler.getRetriedEntries()).isEmpty();
        verify(ignoredResponsesCounter).inc(1);
        verify(status404Counter).inc(1);
        verify(errorCounter, never()).inc(anyLong());
    }

    @Test
    public void testRetryExhaustionRecordsMetrics() throws InterruptedException {
        HttpSinkRequestEntry request = new HttpSinkRequestEntry("PUT", "hello".getBytes());
        when(httpClient.putRequests(anyList(), anyString()))
                .thenReturn(
                        CompletableFuture.completedFuture(
                                new SinkHttpClientResponses(
                                        Collections.emptyList(),
                                        Collections.singletonList(request),
                                        Collections.emptyList(),
                                        Collections.emptyList(),
                                        statistics(2, Map.of(503, 2), 2, 0, 1))));

        RecordingResultHandler resultHandler = new RecordingResultHandler();
        this.httpSinkWriter.submitRequestEntries(Collections.singletonList(request), resultHandler);

        assertThat(resultHandler.await()).isTrue();
        assertThat(resultHandler.getFailure()).hasMessageContaining("exhausted retries");
        verify(httpRequestsCounter).inc(2);
        verify(status503Counter).inc(2);
        verify(retryableResponseFailuresCounter).inc(2);
        verify(retryAttemptsCounter).inc(1);
        verify(retryExhaustedCounter).inc(1);
        verify(errorCounter).inc(1);
    }

    @Test
    public void testTransportFailureWithoutResponseRecordsRequestExceptions()
            throws InterruptedException {
        HttpSinkRequestEntry request = new HttpSinkRequestEntry("PUT", "hello".getBytes());
        when(httpClient.putRequests(anyList(), anyString()))
                .thenReturn(
                        CompletableFuture.completedFuture(
                                new SinkHttpClientResponses(
                                        Collections.emptyList(),
                                        Collections.singletonList(request),
                                        Collections.emptyList(),
                                        Collections.emptyList(),
                                        statistics(1, Map.of(), 1, 1, 0))));

        RecordingResultHandler resultHandler = new RecordingResultHandler();
        this.httpSinkWriter.submitRequestEntries(Collections.singletonList(request), resultHandler);

        assertThat(resultHandler.await()).isTrue();
        assertThat(resultHandler.getFailure()).hasMessageContaining("exhausted retries");
        verify(requestExceptionsCounter).inc(1);
        verify(retryExhaustedCounter).inc(1);
    }

    @Test
    public void testStatusCodeCounterIsRegisteredOnceAndReused() throws InterruptedException {
        HttpSinkRequestEntry request = new HttpSinkRequestEntry("PUT", "hello".getBytes());
        when(httpClient.putRequests(anyList(), anyString()))
                .thenReturn(
                        CompletableFuture.completedFuture(
                                new SinkHttpClientResponses(
                                        Collections.singletonList(request),
                                        Collections.emptyList(),
                                        Collections.emptyList(),
                                        Collections.emptyList(),
                                        statistics(1, Map.of(200, 1), 0, 0, 0))));

        for (int i = 0; i < 2; i++) {
            RecordingResultHandler resultHandler = new RecordingResultHandler();
            this.httpSinkWriter.submitRequestEntries(
                    Collections.singletonList(request), resultHandler);
            assertThat(resultHandler.await()).isTrue();
        }

        verify(statusCodeMetricGroup, times(1)).counter("200");
        verify(status200Counter, times(2)).inc(1);
    }

    private static SinkHttpClientStatistics statistics(
            int httpRequestCount,
            Map<Integer, Integer> statusCodeCounts,
            int retryableResponseEntries,
            int transportFailedEntries,
            int retryAttemptEntries) {
        return new SinkHttpClientStatistics(
                httpRequestCount,
                Collections.emptyList(),
                statusCodeCounts,
                retryableResponseEntries,
                transportFailedEntries,
                retryAttemptEntries);
    }

    @Test
    public void testCloseClosesHttpClient() {
        httpSinkWriter.close();

        verify(httpClient).close();
    }

    private HttpSinkWriter<String> createWriter(
            Collection<BufferedRequestState<HttpSinkRequestEntry>> stateBuffer) {
        Properties properties = new Properties();
        return new HttpSinkWriter<>(
                elementConverter,
                context,
                10,
                10,
                100,
                10,
                10,
                10,
                "http://localhost/client",
                httpClient,
                stateBuffer,
                HttpSinkConfigFactory.fromDataStream(
                        "http://localhost/client", properties, new Slf4jHttpPostRequestCallback()));
    }

    private static class RecordingResultHandler implements ResultHandler<HttpSinkRequestEntry> {
        private final CountDownLatch latch = new CountDownLatch(1);
        private final AtomicReference<List<HttpSinkRequestEntry>> retriedEntries =
                new AtomicReference<>(Collections.emptyList());
        private final AtomicReference<Exception> failure = new AtomicReference<>();

        @Override
        public void complete() {
            latch.countDown();
        }

        @Override
        public void completeExceptionally(Exception e) {
            failure.set(e);
            latch.countDown();
        }

        @Override
        public void retryForEntries(List<HttpSinkRequestEntry> requestEntriesToRetry) {
            retriedEntries.set(requestEntriesToRetry);
            latch.countDown();
        }

        boolean await() throws InterruptedException {
            return latch.await(5, TimeUnit.SECONDS);
        }

        List<HttpSinkRequestEntry> getRetriedEntries() {
            return retriedEntries.get();
        }

        Exception getFailure() {
            return failure.get();
        }
    }
}
