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

import org.apache.flink.api.connector.sink2.Sink.InitContext;
import org.apache.flink.connector.base.sink.writer.BufferedRequestState;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.connector.http.clients.SinkHttpClient;
import org.apache.flink.connector.http.clients.SinkHttpClientResponse;
import org.apache.flink.connector.http.sink.httpclient.HttpRequest;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.groups.OperatorIOMetricGroup;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Test for {@link HttpSinkWriter }. */
@Slf4j
@ExtendWith(MockitoExtension.class)
class HttpSinkWriterTest {

    private HttpSinkWriter<String> httpSinkWriter;

    @Mock private ElementConverter<String, HttpSinkRequestEntry> elementConverter;

    @Mock private InitContext context;

    @Mock private SinkHttpClient httpClient;

    // To work with Flink 1.15 and Flink 1.16
    @Mock(lenient = true)
    private SinkWriterMetricGroup metricGroup;

    @Mock private OperatorIOMetricGroup operatorIOMetricGroup;

    @Mock private Counter errorCounter;

    @BeforeEach
    public void setUp() {
        when(metricGroup.getNumRecordsSendErrorsCounter()).thenReturn(errorCounter);
        when(metricGroup.getIOMetricGroup()).thenReturn(operatorIOMetricGroup);
        when(context.metricGroup()).thenReturn(metricGroup);

        Collection<BufferedRequestState<HttpSinkRequestEntry>> stateBuffer = new ArrayList<>();

        Properties noRetryProperties = new Properties();
        noRetryProperties.setProperty(
                org.apache.flink.connector.http.config.HttpConnectorConfigConstants
                        .SINK_HTTP_RETRY_TIMES,
                "0");

        this.httpSinkWriter =
                new HttpSinkWriter<>(
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
                        noRetryProperties);
    }

    @Test
    public void testErrorMetric() throws InterruptedException {

        CompletableFuture<SinkHttpClientResponse> future = new CompletableFuture<>();
        future.completeExceptionally(new Exception("Test Exception"));

        when(httpClient.putRequests(anyList(), anyString())).thenReturn(future);

        HttpSinkRequestEntry request = new HttpSinkRequestEntry("PUT", "hello".getBytes());
        Consumer<List<HttpSinkRequestEntry>> requestResult =
                httpSinkRequestEntries -> log.info(String.valueOf(httpSinkRequestEntries));

        List<HttpSinkRequestEntry> requestEntries = Collections.singletonList(request);
        this.httpSinkWriter.submitRequestEntries(requestEntries, requestResult);

        // would be good to use Countdown Latch instead sleep...
        Thread.sleep(2000);
        verify(errorCounter).inc(requestEntries.size());
    }

    @Test
    public void testRetryOnError() throws InterruptedException {
        // default maxRetryTimes is 3, so putRequests will be called 1 + 3 = 4 times
        CompletableFuture<SinkHttpClientResponse> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(new Exception("Connection refused"));

        when(httpClient.putRequests(anyList(), anyString())).thenReturn(failedFuture);

        Properties properties = new Properties();
        // set retry.times = 1 to speed up the test
        properties.setProperty(
                org.apache.flink.connector.http.config.HttpConnectorConfigConstants
                        .SINK_HTTP_RETRY_TIMES,
                "1");

        Collection<
                        org.apache.flink.connector.base.sink.writer.BufferedRequestState<
                                HttpSinkRequestEntry>>
                stateBuffer = new ArrayList<>();
        HttpSinkWriter<String> writerWithRetry =
                new HttpSinkWriter<>(
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
                        properties);

        HttpSinkRequestEntry request = new HttpSinkRequestEntry("PUT", "hello".getBytes());
        Consumer<List<HttpSinkRequestEntry>> requestResult =
                httpSinkRequestEntries -> log.info(String.valueOf(httpSinkRequestEntries));

        List<HttpSinkRequestEntry> requestEntries = Collections.singletonList(request);
        writerWithRetry.submitRequestEntries(requestEntries, requestResult);

        // Wait for 1 initial attempt + 1 retry + exponential backoff (1s) + buffer
        Thread.sleep(4000);
        // 1 initial attempt + 1 retry = 2 total calls
        verify(httpClient, times(2)).putRequests(anyList(), anyString());
        verify(errorCounter).inc(requestEntries.size());
    }

    @Test
    public void testRetryOnHttpFailedRequests() throws InterruptedException {
        // Simulate HTTP-level failure: server returns failed requests in response
        HttpSinkRequestEntry request = new HttpSinkRequestEntry("PUT", "hello".getBytes());
        List<HttpSinkRequestEntry> requestEntries = Collections.singletonList(request);

        // Build a mock HttpRequest to put in the failed list
        HttpRequest mockHttpRequest =
                new HttpRequest(null, Collections.singletonList("hello".getBytes()), "PUT");

        // First call: returns HTTP-level failure, second call: returns success
        SinkHttpClientResponse failedResponse =
                new SinkHttpClientResponse(
                        Collections.emptyList(), Collections.singletonList(mockHttpRequest));
        SinkHttpClientResponse successResponse =
                new SinkHttpClientResponse(
                        Collections.singletonList(mockHttpRequest), Collections.emptyList());

        when(httpClient.putRequests(anyList(), anyString()))
                .thenReturn(CompletableFuture.completedFuture(failedResponse))
                .thenReturn(CompletableFuture.completedFuture(successResponse));

        Properties properties = new Properties();
        properties.setProperty(
                org.apache.flink.connector.http.config.HttpConnectorConfigConstants
                        .SINK_HTTP_RETRY_TIMES,
                "2");

        Collection<BufferedRequestState<HttpSinkRequestEntry>> stateBuffer = new ArrayList<>();
        HttpSinkWriter<String> writerWithRetry =
                new HttpSinkWriter<>(
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
                        properties);

        AtomicInteger acceptCallCount = new AtomicInteger(0);
        Consumer<List<HttpSinkRequestEntry>> requestResult =
                httpSinkRequestEntries -> acceptCallCount.incrementAndGet();

        writerWithRetry.submitRequestEntries(requestEntries, requestResult);

        // Wait for retry backoff (1s) + buffer
        Thread.sleep(3000);

        // Should have retried once and then succeeded: 2 total calls
        verify(httpClient, times(2)).putRequests(anyList(), anyString());
        // No error counted since eventually succeeded
        verify(errorCounter, times(0)).inc(requestEntries.size());
        // requestResult.accept() called exactly once
        assertThat(acceptCallCount.get()).isEqualTo(1);
    }

    @Test
    public void testNoRetryWhenDisabled() throws InterruptedException {
        // retry.times=0 should not retry at all (already covered by setUp, explicit test here)
        CompletableFuture<SinkHttpClientResponse> future = new CompletableFuture<>();
        future.completeExceptionally(new Exception("Connection refused"));

        when(httpClient.putRequests(anyList(), anyString())).thenReturn(future);

        HttpSinkRequestEntry request = new HttpSinkRequestEntry("PUT", "hello".getBytes());
        List<HttpSinkRequestEntry> requestEntries = Collections.singletonList(request);

        AtomicInteger acceptCallCount = new AtomicInteger(0);
        Consumer<List<HttpSinkRequestEntry>> requestResult =
                httpSinkRequestEntries -> acceptCallCount.incrementAndGet();

        // httpSinkWriter is created with retry.times=0 in setUp
        this.httpSinkWriter.submitRequestEntries(requestEntries, requestResult);

        Thread.sleep(1000);

        // Only 1 attempt, no retries
        verify(httpClient, times(1)).putRequests(anyList(), anyString());
        verify(errorCounter).inc(requestEntries.size());
        // requestResult.accept() called exactly once
        assertThat(acceptCallCount.get()).isEqualTo(1);
    }
}
