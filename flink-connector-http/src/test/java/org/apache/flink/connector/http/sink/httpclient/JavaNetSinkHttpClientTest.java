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

package org.apache.flink.connector.http.sink.httpclient;

import org.apache.flink.connector.http.HttpPostRequestCallback;
import org.apache.flink.connector.http.clients.SinkHttpClientResponses;
import org.apache.flink.connector.http.clients.SinkHttpClientStatistics;
import org.apache.flink.connector.http.config.HttpConnectorConfigConstants;
import org.apache.flink.connector.http.config.HttpSinkConfig;
import org.apache.flink.connector.http.config.HttpSinkConfigFactory;
import org.apache.flink.connector.http.preprocessor.HeaderPreprocessor;
import org.apache.flink.connector.http.sink.HttpSinkRequestEntry;
import org.apache.flink.connector.http.table.sink.Slf4jHttpPostRequestCallback;
import org.apache.flink.connector.http.utils.HttpHeaderUtils;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.apache.flink.connector.http.TestHelper.assertPropertyArray;
import static org.apache.flink.connector.http.config.HttpConnectorConfigConstants.SINK_MAX_RETRIES;
import static org.apache.flink.connector.http.config.HttpConnectorConfigConstants.SINK_RETRY_FIXED_DELAY_DELAY;
import static org.apache.flink.connector.http.table.sink.HttpDynamicSinkConnectorOptions.SINK_HTTP_IGNORED_RESPONSE_CODES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Test for {@link JavaNetSinkHttpClient }. */
@ExtendWith(MockitoExtension.class)
class JavaNetSinkHttpClientTest {

    private static MockedStatic<HttpClient> httpClientStaticMock;

    @Mock private HttpClient.Builder httpClientBuilder;

    @BeforeAll
    public static void beforeAll() {
        httpClientStaticMock = mockStatic(HttpClient.class);
    }

    protected HeaderPreprocessor headerPreprocessor;

    protected HttpPostRequestCallback<HttpRequest> postRequestCallback;

    @AfterAll
    public static void afterAll() {
        if (httpClientStaticMock != null) {
            httpClientStaticMock.close();
        }
    }

    @BeforeEach
    public void setUp() {
        postRequestCallback = new Slf4jHttpPostRequestCallback();
        headerPreprocessor = HttpHeaderUtils.createBasicAuthorizationHeaderPreprocessor();
        httpClientStaticMock.when(HttpClient::newBuilder).thenReturn(httpClientBuilder);
        lenient().when(httpClientBuilder.followRedirects(any())).thenReturn(httpClientBuilder);
        lenient().when(httpClientBuilder.sslContext(any())).thenReturn(httpClientBuilder);
        lenient().when(httpClientBuilder.executor(any())).thenReturn(httpClientBuilder);
    }

    private final List<JavaNetSinkHttpClient> clientsToClose = new ArrayList<>();

    @AfterEach
    public void closeClients() {
        clientsToClose.forEach(JavaNetSinkHttpClient::close);
        clientsToClose.clear();
    }

    private static Stream<Arguments> provideSubmitterFactory() {
        return Stream.of(
                Arguments.of(new PerRequestRequestSubmitterFactory()),
                Arguments.of(new BatchRequestSubmitterFactory(50)));
    }

    private static HttpSinkConfig sinkConfig(Properties properties) {
        return sinkConfig(properties, new Slf4jHttpPostRequestCallback());
    }

    private static HttpSinkConfig sinkConfig(
            Properties properties, HttpPostRequestCallback<HttpRequest> callback) {
        return HttpSinkConfigFactory.fromDataStream("http://localhost", properties, callback);
    }

    @ParameterizedTest
    @MethodSource("provideSubmitterFactory")
    public void shouldBuildClientWithoutHeaders(RequestSubmitterFactory requestSubmitterFactory) {

        JavaNetSinkHttpClient client =
                new JavaNetSinkHttpClient(
                        sinkConfig(new Properties()), headerPreprocessor, requestSubmitterFactory);
        assertThat(client.getHeadersAndValues()).isEmpty();
    }

    @ParameterizedTest
    @MethodSource("provideSubmitterFactory")
    public void shouldBuildClientWithHeaders(RequestSubmitterFactory requestSubmitterFactory) {

        // GIVEN
        Properties properties = new Properties();
        properties.setProperty("property", "val1");
        properties.setProperty("my.property", "val2");
        properties.setProperty(
                HttpConnectorConfigConstants.SINK_HEADER_PREFIX + "Origin",
                "https://developer.mozilla.org");
        properties.setProperty(
                HttpConnectorConfigConstants.SINK_HEADER_PREFIX + "Cache-Control",
                "no-cache, no-store, max-age=0, must-revalidate");
        properties.setProperty(
                HttpConnectorConfigConstants.SINK_HEADER_PREFIX + "Access-Control-Allow-Origin",
                "*");

        // WHEN
        JavaNetSinkHttpClient client =
                new JavaNetSinkHttpClient(
                        sinkConfig(properties), headerPreprocessor, requestSubmitterFactory);
        String[] headersAndValues = client.getHeadersAndValues();
        assertThat(headersAndValues).hasSize(6);

        // THEN
        // assert that we have property followed by its value.
        assertPropertyArray(headersAndValues, "Origin", "https://developer.mozilla.org");
        assertPropertyArray(
                headersAndValues,
                "Cache-Control",
                "no-cache, no-store, max-age=0, must-revalidate");
        assertPropertyArray(headersAndValues, "Access-Control-Allow-Origin", "*");
    }

    @Test
    public void shouldClassifySinkResponses() {
        HttpSinkRequestEntry successfulEntry = new HttpSinkRequestEntry("POST", new byte[] {1});
        HttpSinkRequestEntry retryableEntry = new HttpSinkRequestEntry("POST", new byte[] {2});
        HttpSinkRequestEntry fatalEntry = new HttpSinkRequestEntry("POST", new byte[] {3});
        HttpSinkRequestEntry ignoredEntry = new HttpSinkRequestEntry("POST", new byte[] {4});

        RequestSubmitterFactory submitterFactory =
                (_sinkConfig, _headersAndValues) ->
                        (_endpointUrl, _requestToSubmit) ->
                                List.of(
                                        responseFuture(successfulEntry, 200),
                                        responseFuture(retryableEntry, 500),
                                        responseFuture(fatalEntry, 400),
                                        responseFuture(ignoredEntry, 404));

        Properties properties = new Properties();
        properties.setProperty(SINK_MAX_RETRIES, "0");
        properties.setProperty(SINK_HTTP_IGNORED_RESPONSE_CODES.key(), "404");
        JavaNetSinkHttpClient client =
                new JavaNetSinkHttpClient(
                        sinkConfig(properties), headerPreprocessor, submitterFactory);

        var response =
                client.putRequests(
                                List.of(successfulEntry, retryableEntry, fatalEntry, ignoredEntry),
                                "http://localhost")
                        .join();

        assertThat(response.getSuccessfulRequests()).containsExactly(successfulEntry);
        assertThat(response.getIgnoredRequests()).containsExactly(ignoredEntry);
        assertThat(response.getRetriableFailedRequests()).containsExactly(retryableEntry);
        assertThat(response.getFatalFailedRequests()).containsExactly(fatalEntry);
    }

    @Test
    public void shouldRetryRetryableResponsesInsideHttpClient() {
        HttpSinkRequestEntry retryableEntry = new HttpSinkRequestEntry("POST", new byte[] {1});
        AtomicInteger calls = new AtomicInteger();

        RequestSubmitterFactory submitterFactory =
                (_sinkConfig, _headersAndValues) ->
                        (_endpointUrl, requestToSubmit) -> {
                            assertThat(requestToSubmit).containsExactly(retryableEntry);
                            int statusCode = calls.getAndIncrement() == 0 ? 500 : 200;
                            return List.of(responseFuture(retryableEntry, statusCode));
                        };

        Properties properties = new Properties();
        properties.setProperty(SINK_MAX_RETRIES, "1");
        properties.setProperty(SINK_RETRY_FIXED_DELAY_DELAY, "1ms");
        JavaNetSinkHttpClient client =
                new JavaNetSinkHttpClient(
                        sinkConfig(properties), headerPreprocessor, submitterFactory);

        var response = client.putRequests(List.of(retryableEntry), "http://localhost").join();

        assertThat(response.getSuccessfulRequests()).containsExactly(retryableEntry);
        assertThat(response.getRetriableFailedRequests()).isEmpty();
        assertThat(response.getFatalFailedRequests()).isEmpty();
        assertThat(calls).hasValue(2);
    }

    @Test
    public void shouldRetryOnlyRetryableEntriesFromMixedAttempt() {
        HttpSinkRequestEntry successfulEntry = new HttpSinkRequestEntry("POST", new byte[] {1});
        HttpSinkRequestEntry retryableEntry = new HttpSinkRequestEntry("POST", new byte[] {2});
        AtomicInteger calls = new AtomicInteger();

        RequestSubmitterFactory submitterFactory =
                (_sinkConfig, _headersAndValues) ->
                        (_endpointUrl, requestToSubmit) -> {
                            if (calls.getAndIncrement() == 0) {
                                assertThat(requestToSubmit)
                                        .containsExactly(successfulEntry, retryableEntry);
                                return List.of(
                                        responseFuture(successfulEntry, 200),
                                        responseFuture(retryableEntry, 500));
                            }
                            assertThat(requestToSubmit).containsExactly(retryableEntry);
                            return List.of(responseFuture(retryableEntry, 200));
                        };

        Properties properties = new Properties();
        properties.setProperty(SINK_MAX_RETRIES, "1");
        properties.setProperty(SINK_RETRY_FIXED_DELAY_DELAY, "1ms");
        JavaNetSinkHttpClient client =
                new JavaNetSinkHttpClient(
                        sinkConfig(properties), headerPreprocessor, submitterFactory);

        var response =
                client.putRequests(List.of(successfulEntry, retryableEntry), "http://localhost")
                        .join();

        assertThat(response.getSuccessfulRequests())
                .containsExactly(successfulEntry, retryableEntry);
        assertThat(response.getRetriableFailedRequests()).isEmpty();
        assertThat(response.getFatalFailedRequests()).isEmpty();
        assertThat(calls).hasValue(2);
    }

    @Test
    public void shouldReturnRetryableFailureOnlyAfterHttpClientRetriesAreExhausted() {
        HttpSinkRequestEntry retryableEntry = new HttpSinkRequestEntry("POST", new byte[] {1});
        AtomicInteger calls = new AtomicInteger();

        RequestSubmitterFactory submitterFactory =
                (_sinkConfig, _headersAndValues) ->
                        (_endpointUrl, requestToSubmit) -> {
                            assertThat(requestToSubmit).containsExactly(retryableEntry);
                            calls.incrementAndGet();
                            return List.of(responseFuture(retryableEntry, 500));
                        };

        Properties properties = new Properties();
        properties.setProperty(SINK_MAX_RETRIES, "2");
        properties.setProperty(SINK_RETRY_FIXED_DELAY_DELAY, "1ms");
        JavaNetSinkHttpClient client =
                new JavaNetSinkHttpClient(
                        sinkConfig(properties), headerPreprocessor, submitterFactory);

        var response = client.putRequests(List.of(retryableEntry), "http://localhost").join();

        assertThat(response.getSuccessfulRequests()).isEmpty();
        assertThat(response.getRetriableFailedRequests()).containsExactly(retryableEntry);
        assertThat(response.getFatalFailedRequests()).isEmpty();
        assertThat(calls).hasValue(3);
    }

    @Test
    public void shouldRetryIoExceptionFromFailedCompletionStage() {
        HttpSinkRequestEntry retryableEntry = new HttpSinkRequestEntry("POST", new byte[] {1});
        AtomicInteger calls = new AtomicInteger();

        RequestSubmitterFactory submitterFactory =
                (_sinkConfig, _headersAndValues) ->
                        (_endpointUrl, requestToSubmit) -> {
                            assertThat(requestToSubmit).containsExactly(retryableEntry);
                            if (calls.getAndIncrement() == 0) {
                                return List.of(
                                        CompletableFuture.failedFuture(
                                                new java.io.IOException("connection reset")));
                            }
                            return List.of(responseFuture(retryableEntry, 200));
                        };

        Properties properties = new Properties();
        properties.setProperty(SINK_MAX_RETRIES, "1");
        properties.setProperty(SINK_RETRY_FIXED_DELAY_DELAY, "1ms");
        JavaNetSinkHttpClient client =
                new JavaNetSinkHttpClient(
                        sinkConfig(properties), headerPreprocessor, submitterFactory);

        var response = client.putRequests(List.of(retryableEntry), "http://localhost").join();

        assertThat(response.getSuccessfulRequests()).containsExactly(retryableEntry);
        assertThat(response.getRetriableFailedRequests()).isEmpty();
        assertThat(response.getFatalFailedRequests()).isEmpty();
        assertThat(calls).hasValue(2);
    }

    @Test
    public void shouldNotRetryRuntimeExceptionFromCallback() {
        HttpSinkRequestEntry retryableEntry = new HttpSinkRequestEntry("POST", new byte[] {1});
        AtomicInteger calls = new AtomicInteger();
        HttpPostRequestCallback<HttpRequest> throwingCallback =
                (_response, _request, _endpointUrl, _headerMap) -> {
                    throw new RuntimeException("callback failed");
                };

        RequestSubmitterFactory submitterFactory =
                (_sinkConfig, _headersAndValues) ->
                        (_endpointUrl, requestToSubmit) -> {
                            assertThat(requestToSubmit).containsExactly(retryableEntry);
                            calls.incrementAndGet();
                            return List.of(responseFutureWithoutStatus(retryableEntry));
                        };

        Properties properties = new Properties();
        properties.setProperty(SINK_MAX_RETRIES, "1");
        properties.setProperty(SINK_RETRY_FIXED_DELAY_DELAY, "1ms");
        JavaNetSinkHttpClient client =
                new JavaNetSinkHttpClient(
                        sinkConfig(properties, throwingCallback),
                        headerPreprocessor,
                        submitterFactory);

        assertThatThrownBy(
                        () ->
                                client.putRequests(List.of(retryableEntry), "http://localhost")
                                        .join())
                .hasRootCauseInstanceOf(RuntimeException.class)
                .hasRootCauseMessage("callback failed");
        assertThat(calls).hasValue(1);
    }

    @Test
    public void closeClosesRequestSubmitter() {
        AtomicBoolean submitterClosed = new AtomicBoolean();
        RequestSubmitterFactory submitterFactory =
                (_sinkConfig, _headersAndValues) ->
                        new RequestSubmitter() {
                            @Override
                            public List<CompletableFuture<JavaNetHttpResponseWrapper>> submit(
                                    String endpointUrl,
                                    List<HttpSinkRequestEntry> requestToSubmit) {
                                return List.of();
                            }

                            @Override
                            public void close() {
                                submitterClosed.set(true);
                            }
                        };

        JavaNetSinkHttpClient client =
                new JavaNetSinkHttpClient(
                        sinkConfig(new Properties()), headerPreprocessor, submitterFactory);

        client.close();

        assertThat(submitterClosed).isTrue();
    }

    @Test
    public void perRequestModeRetriesOnlyTheFailedEntry() {
        HttpSinkRequestEntry successfulEntry = new HttpSinkRequestEntry("POST", new byte[] {1});
        HttpSinkRequestEntry retryableEntry = new HttpSinkRequestEntry("POST", new byte[] {2});
        HttpClient httpClient = mock(HttpClient.class);
        doReturn(httpResponse(200), httpResponse(500), httpResponse(200))
                .when(httpClient)
                .sendAsync(any(), any());
        List<List<HttpSinkRequestEntry>> attempts = new CopyOnWriteArrayList<>();

        Properties properties = new Properties();
        properties.setProperty(SINK_MAX_RETRIES, "1");
        properties.setProperty(SINK_RETRY_FIXED_DELAY_DELAY, "1ms");
        JavaNetSinkHttpClient client =
                retryingClient(
                        sinkConfig(properties),
                        recordingSubmitterFactory(httpClient, false, attempts));

        var response =
                client.putRequests(List.of(successfulEntry, retryableEntry), "http://localhost")
                        .join();

        assertThat(attempts).hasSize(2);
        assertSameEntries(attempts.get(0), successfulEntry, retryableEntry);
        assertSameEntries(attempts.get(1), retryableEntry);
        assertSameEntries(response.getSuccessfulRequests(), successfulEntry, retryableEntry);
        assertThat(response.getRetriableFailedRequests()).isEmpty();
        assertThat(response.getFatalFailedRequests()).isEmpty();
    }

    @Test
    public void batchModeRetriesAllOriginalEntriesOfRetryableBatch() {
        List<HttpSinkRequestEntry> batch = batchEntries();
        HttpClient httpClient = mock(HttpClient.class);
        doReturn(httpResponse(500), httpResponse(200)).when(httpClient).sendAsync(any(), any());
        List<List<HttpSinkRequestEntry>> attempts = new CopyOnWriteArrayList<>();

        JavaNetSinkHttpClient client =
                retryingClient(
                        sinkConfig(batchProperties("1")),
                        recordingSubmitterFactory(httpClient, true, attempts));

        var response = client.putRequests(batch, "http://localhost").join();

        verify(httpClient, times(2)).sendAsync(any(), any());
        assertThat(attempts).hasSize(2);
        assertSameEntries(attempts.get(0), batch.toArray(new HttpSinkRequestEntry[0]));
        assertSameEntries(attempts.get(1), batch.toArray(new HttpSinkRequestEntry[0]));
        assertSameEntries(
                response.getSuccessfulRequests(), batch.toArray(new HttpSinkRequestEntry[0]));
        assertThat(response.getRetriableFailedRequests()).isEmpty();
        assertThat(response.getFatalFailedRequests()).isEmpty();
    }

    @Test
    public void batchModeReturnsAllEntriesAsRetryableWhenRetriesAreExhausted() {
        List<HttpSinkRequestEntry> batch = batchEntries();
        HttpClient httpClient = mock(HttpClient.class);
        doReturn(httpResponse(500), httpResponse(500)).when(httpClient).sendAsync(any(), any());
        List<List<HttpSinkRequestEntry>> attempts = new CopyOnWriteArrayList<>();

        JavaNetSinkHttpClient client =
                retryingClient(
                        sinkConfig(batchProperties("1")),
                        recordingSubmitterFactory(httpClient, true, attempts));

        var response = client.putRequests(batch, "http://localhost").join();

        assertThat(attempts).hasSize(2);
        assertSameEntries(attempts.get(1), batch.toArray(new HttpSinkRequestEntry[0]));
        assertThat(response.getSuccessfulRequests()).isEmpty();
        assertSameEntries(
                response.getRetriableFailedRequests(), batch.toArray(new HttpSinkRequestEntry[0]));
        assertThat(response.getFatalFailedRequests()).isEmpty();
    }

    @Test
    public void batchModeFailsAllEntriesOfFatalBatchWithoutRetry() {
        List<HttpSinkRequestEntry> batch = batchEntries();
        HttpClient httpClient = mock(HttpClient.class);
        doReturn(httpResponse(400)).when(httpClient).sendAsync(any(), any());
        List<List<HttpSinkRequestEntry>> attempts = new CopyOnWriteArrayList<>();

        JavaNetSinkHttpClient client =
                retryingClient(
                        sinkConfig(batchProperties("1")),
                        recordingSubmitterFactory(httpClient, true, attempts));

        var response = client.putRequests(batch, "http://localhost").join();

        verify(httpClient, times(1)).sendAsync(any(), any());
        assertThat(attempts).hasSize(1);
        assertThat(response.getSuccessfulRequests()).isEmpty();
        assertThat(response.getRetriableFailedRequests()).isEmpty();
        assertSameEntries(
                response.getFatalFailedRequests(), batch.toArray(new HttpSinkRequestEntry[0]));
    }

    @Test
    public void batchModeCompletesAllEntriesOfIgnoredBatch() {
        List<HttpSinkRequestEntry> batch = batchEntries();
        HttpClient httpClient = mock(HttpClient.class);
        doReturn(httpResponse(404)).when(httpClient).sendAsync(any(), any());
        List<List<HttpSinkRequestEntry>> attempts = new CopyOnWriteArrayList<>();

        Properties properties = batchProperties("1");
        properties.setProperty(SINK_HTTP_IGNORED_RESPONSE_CODES.key(), "404");
        JavaNetSinkHttpClient client =
                retryingClient(
                        sinkConfig(properties),
                        recordingSubmitterFactory(httpClient, true, attempts));

        var response = client.putRequests(batch, "http://localhost").join();

        assertThat(attempts).hasSize(1);
        assertSameEntries(
                response.getIgnoredRequests(), batch.toArray(new HttpSinkRequestEntry[0]));
        assertThat(response.getRetriableFailedRequests()).isEmpty();
        assertThat(response.getFatalFailedRequests()).isEmpty();
    }

    @Test
    public void batchModeRetriesOnlyTheFailedRequestWhenFlushIsSplit() {
        HttpSinkRequestEntry first = new HttpSinkRequestEntry("POST", new byte[] {1});
        HttpSinkRequestEntry second = new HttpSinkRequestEntry("POST", new byte[] {2});
        HttpSinkRequestEntry third = new HttpSinkRequestEntry("POST", new byte[] {3});
        HttpClient httpClient = mock(HttpClient.class);
        // first attempt: [first, second] -> 500, [third] -> 200; retry: [first, second] -> 200
        doReturn(httpResponse(500), httpResponse(200), httpResponse(200))
                .when(httpClient)
                .sendAsync(any(), any());
        List<List<HttpSinkRequestEntry>> attempts = new CopyOnWriteArrayList<>();
        List<List<HttpSinkRequestEntry>> httpRequests = new CopyOnWriteArrayList<>();

        Properties properties = batchProperties("1");
        properties.setProperty(HttpConnectorConfigConstants.SINK_HTTP_BATCH_REQUEST_SIZE, "2");
        JavaNetSinkHttpClient client =
                retryingClient(
                        sinkConfig(properties, recordingCallback(httpRequests)),
                        recordingSubmitterFactory(httpClient, true, attempts));

        var response = client.putRequests(List.of(first, second, third), "http://localhost").join();

        assertThat(attempts).hasSize(2);
        assertSameEntries(attempts.get(0), first, second, third);
        assertSameEntries(attempts.get(1), first, second);
        assertThat(httpRequests).hasSize(3);
        assertSameEntries(httpRequests.get(0), first, second);
        assertSameEntries(httpRequests.get(1), third);
        assertSameEntries(httpRequests.get(2), first, second);
        assertSameEntriesInAnyOrder(response.getSuccessfulRequests(), first, second, third);
        assertThat(response.getRetriableFailedRequests()).isEmpty();
        assertThat(response.getFatalFailedRequests()).isEmpty();
    }

    @Test
    public void batchModeRegroupsRetriedEntriesIntoNewHttpRequests() {
        HttpSinkRequestEntry firstPost = new HttpSinkRequestEntry("POST", new byte[] {1});
        HttpSinkRequestEntry put = new HttpSinkRequestEntry("PUT", new byte[] {2});
        HttpSinkRequestEntry secondPost = new HttpSinkRequestEntry("POST", new byte[] {3});
        HttpClient httpClient = mock(HttpClient.class);
        // first attempt splits on the method change: [firstPost] -> 500, [put] -> 200,
        // [secondPost] -> 500; the retry sends both POST entries as one request -> 200
        doReturn(httpResponse(500), httpResponse(200), httpResponse(500), httpResponse(200))
                .when(httpClient)
                .sendAsync(any(), any());
        List<List<HttpSinkRequestEntry>> attempts = new CopyOnWriteArrayList<>();
        List<List<HttpSinkRequestEntry>> httpRequests = new CopyOnWriteArrayList<>();

        JavaNetSinkHttpClient client =
                retryingClient(
                        sinkConfig(batchProperties("1"), recordingCallback(httpRequests)),
                        recordingSubmitterFactory(httpClient, true, attempts));

        var response =
                client.putRequests(List.of(firstPost, put, secondPost), "http://localhost").join();

        verify(httpClient, times(4)).sendAsync(any(), any());
        assertThat(attempts).hasSize(2);
        assertSameEntries(attempts.get(1), firstPost, secondPost);
        assertThat(httpRequests).hasSize(4);
        assertSameEntries(httpRequests.get(0), firstPost);
        assertSameEntries(httpRequests.get(1), put);
        assertSameEntries(httpRequests.get(2), secondPost);
        assertSameEntries(httpRequests.get(3), firstPost, secondPost);
        assertSameEntriesInAnyOrder(response.getSuccessfulRequests(), firstPost, put, secondPost);
        assertThat(response.getRetriableFailedRequests()).isEmpty();
        assertThat(response.getFatalFailedRequests()).isEmpty();
    }

    private JavaNetSinkHttpClient retryingClient(
            HttpSinkConfig sinkConfig, RequestSubmitterFactory requestSubmitterFactory) {
        JavaNetSinkHttpClient client =
                new JavaNetSinkHttpClient(sinkConfig, headerPreprocessor, requestSubmitterFactory);
        clientsToClose.add(client);
        return client;
    }

    @Test
    public void shouldCollectStatisticsForClassifiedResponses() {
        HttpSinkRequestEntry successfulEntry = new HttpSinkRequestEntry("POST", new byte[] {1});
        HttpSinkRequestEntry retryableEntry = new HttpSinkRequestEntry("POST", new byte[] {2});
        HttpSinkRequestEntry fatalEntry = new HttpSinkRequestEntry("POST", new byte[] {3});
        HttpSinkRequestEntry ignoredEntry = new HttpSinkRequestEntry("POST", new byte[] {4});

        RequestSubmitterFactory submitterFactory =
                (_sinkConfig, _headersAndValues) ->
                        (_endpointUrl, _requestToSubmit) ->
                                List.of(
                                        responseFuture(successfulEntry, 200, 10),
                                        responseFuture(retryableEntry, 500, 20),
                                        responseFuture(fatalEntry, 400, 30),
                                        responseFuture(ignoredEntry, 404, 40));

        Properties properties = new Properties();
        properties.setProperty(SINK_MAX_RETRIES, "0");
        properties.setProperty(SINK_HTTP_IGNORED_RESPONSE_CODES.key(), "404");
        JavaNetSinkHttpClient client = retryingClient(sinkConfig(properties), submitterFactory);

        var response =
                client.putRequests(
                                List.of(successfulEntry, retryableEntry, fatalEntry, ignoredEntry),
                                "http://localhost")
                        .join();

        SinkHttpClientStatistics statistics = response.getStatistics();
        assertThat(statistics.getHttpRequestCount()).isEqualTo(4);
        assertThat(statistics.getRequestLatenciesMillis()).containsExactly(10L, 20L, 30L, 40L);
        assertThat(statistics.getStatusCodeCounts())
                .hasSize(4)
                .containsEntry(200, 1)
                .containsEntry(500, 1)
                .containsEntry(400, 1)
                .containsEntry(404, 1);
        assertThat(statistics.getRetryableResponseEntries()).isEqualTo(1);
        assertThat(statistics.getTransportFailedEntries()).isZero();
        assertThat(statistics.getRetryAttemptEntries()).isZero();
    }

    @Test
    public void shouldCountTransportFailuresInStatistics() {
        HttpSinkRequestEntry entry = new HttpSinkRequestEntry("POST", new byte[] {1});

        RequestSubmitterFactory submitterFactory =
                (_sinkConfig, _headersAndValues) ->
                        (_endpointUrl, _requestToSubmit) ->
                                List.of(responseFutureWithoutResponse(entry));

        Properties properties = new Properties();
        properties.setProperty(SINK_MAX_RETRIES, "0");
        JavaNetSinkHttpClient client = retryingClient(sinkConfig(properties), submitterFactory);

        var response = client.putRequests(List.of(entry), "http://localhost").join();

        assertThat(response.getRetriableFailedRequests()).containsExactly(entry);
        SinkHttpClientStatistics statistics = response.getStatistics();
        assertThat(statistics.getHttpRequestCount()).isEqualTo(1);
        assertThat(statistics.getRequestLatenciesMillis()).isEmpty();
        assertThat(statistics.getStatusCodeCounts()).isEmpty();
        assertThat(statistics.getRetryableResponseEntries()).isEqualTo(1);
        assertThat(statistics.getTransportFailedEntries()).isEqualTo(1);
    }

    @Test
    public void shouldCountRetryAttemptsInStatistics() {
        HttpSinkRequestEntry successfulEntry = new HttpSinkRequestEntry("POST", new byte[] {1});
        HttpSinkRequestEntry retryableEntry = new HttpSinkRequestEntry("POST", new byte[] {2});
        AtomicInteger attempts = new AtomicInteger();

        RequestSubmitterFactory submitterFactory =
                (_sinkConfig, _headersAndValues) ->
                        (_endpointUrl, _requestToSubmit) -> {
                            if (attempts.getAndIncrement() == 0) {
                                return List.of(
                                        responseFuture(successfulEntry, 200, 5),
                                        responseFuture(retryableEntry, 503, 6));
                            }
                            return List.of(responseFuture(retryableEntry, 200, 7));
                        };

        Properties properties = new Properties();
        properties.setProperty(SINK_MAX_RETRIES, "1");
        properties.setProperty(SINK_RETRY_FIXED_DELAY_DELAY, "1ms");
        JavaNetSinkHttpClient client = retryingClient(sinkConfig(properties), submitterFactory);

        var response =
                client.putRequests(List.of(successfulEntry, retryableEntry), "http://localhost")
                        .join();

        assertThat(response.getSuccessfulRequests())
                .containsExactlyInAnyOrder(successfulEntry, retryableEntry);
        SinkHttpClientStatistics statistics = response.getStatistics();
        assertThat(statistics.getHttpRequestCount()).isEqualTo(3);
        assertThat(statistics.getRequestLatenciesMillis()).containsExactly(5L, 6L, 7L);
        assertThat(statistics.getStatusCodeCounts()).containsEntry(200, 2).containsEntry(503, 1);
        assertThat(statistics.getRetryableResponseEntries()).isEqualTo(1);
        assertThat(statistics.getRetryAttemptEntries()).isEqualTo(1);
    }

    @Test
    public void shouldNotCountExhaustedAttemptAsRetry() {
        HttpSinkRequestEntry retryableEntry = new HttpSinkRequestEntry("POST", new byte[] {1});

        RequestSubmitterFactory submitterFactory =
                (_sinkConfig, _headersAndValues) ->
                        (_endpointUrl, _requestToSubmit) ->
                                List.of(responseFuture(retryableEntry, 503, 1));

        Properties properties = new Properties();
        properties.setProperty(SINK_MAX_RETRIES, "1");
        properties.setProperty(SINK_RETRY_FIXED_DELAY_DELAY, "1ms");
        JavaNetSinkHttpClient client = retryingClient(sinkConfig(properties), submitterFactory);

        var response = client.putRequests(List.of(retryableEntry), "http://localhost").join();

        assertThat(response.getRetriableFailedRequests()).containsExactly(retryableEntry);
        SinkHttpClientStatistics statistics = response.getStatistics();
        assertThat(statistics.getHttpRequestCount()).isEqualTo(2);
        assertThat(statistics.getRetryableResponseEntries()).isEqualTo(2);
        // the second attempt was the last one allowed, so it is exhausted, not retried
        assertThat(statistics.getRetryAttemptEntries()).isEqualTo(1);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void shouldReportTransportFailuresFromSubmitters(boolean batchMode) {
        HttpSinkRequestEntry entry = new HttpSinkRequestEntry("POST", new byte[] {1});
        HttpClient httpClient = mock(HttpClient.class);
        doReturn(CompletableFuture.failedFuture(new IOException("connection refused")))
                .when(httpClient)
                .sendAsync(any(), any());
        List<List<HttpSinkRequestEntry>> attempts = new CopyOnWriteArrayList<>();
        JavaNetSinkHttpClient client =
                retryingClient(
                        sinkConfig(batchProperties("0")),
                        recordingSubmitterFactory(httpClient, batchMode, attempts));

        var response = client.putRequests(List.of(entry), "http://localhost").join();

        assertThat(response.getRetriableFailedRequests()).containsExactly(entry);
        SinkHttpClientStatistics statistics = response.getStatistics();
        assertThat(statistics.getHttpRequestCount()).isEqualTo(1);
        assertThat(statistics.getTransportFailedEntries()).isEqualTo(1);
        assertThat(statistics.getStatusCodeCounts()).isEmpty();
        assertThat(statistics.getRequestLatenciesMillis()).hasSize(1);
    }

    @Test
    public void shouldCountStatusCodesPerRequestEntryOfBatchResponse() {
        List<HttpSinkRequestEntry> batch = batchEntries();

        // one HTTP response for a request that carries all three entries
        RequestSubmitterFactory submitterFactory =
                (_sinkConfig, _headersAndValues) ->
                        (_endpointUrl, _requestToSubmit) -> List.of(responseFuture(batch, 500, 15));

        Properties properties = new Properties();
        properties.setProperty(SINK_MAX_RETRIES, "0");
        JavaNetSinkHttpClient client = retryingClient(sinkConfig(properties), submitterFactory);

        var response = client.putRequests(batch, "http://localhost").join();

        assertThat(response.getRetriableFailedRequests()).hasSize(3);
        SinkHttpClientStatistics statistics = response.getStatistics();
        assertThat(statistics.getHttpRequestCount()).isEqualTo(1);
        assertThat(statistics.getStatusCodeCounts()).containsExactly(entry(500, 3));
        assertThat(statistics.getRetryableResponseEntries()).isEqualTo(3);
        assertThat(statistics.getRequestLatenciesMillis()).containsExactly(15L);
    }

    @Test
    public void shouldReturnEmptyStatisticsFromCompatibilityConstructors() {
        HttpSinkRequestEntry entry = new HttpSinkRequestEntry("POST", new byte[] {1});

        assertThat(
                        new SinkHttpClientResponses(List.of(entry), List.of(), List.of())
                                .getStatistics())
                .isEqualTo(SinkHttpClientStatistics.empty());
        assertThat(
                        new SinkHttpClientResponses(List.of(entry), List.of(), List.of(), List.of())
                                .getStatistics())
                .isEqualTo(SinkHttpClientStatistics.empty());
        assertThat(SinkHttpClientStatistics.empty().getHttpRequestCount()).isZero();
        assertThat(SinkHttpClientStatistics.empty().getStatusCodeCounts()).isEmpty();
    }

    @Test
    public void shouldRejectNullStatisticsCollections() {
        assertThatThrownBy(() -> new SinkHttpClientStatistics(0, null, Map.of(), 0, 0, 0))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new SinkHttpClientStatistics(0, List.of(), null, 0, 0, 0))
                .isInstanceOf(NullPointerException.class);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void submittersKeepTheCauseOfAFailedRequest(boolean batchMode) {
        HttpSinkRequestEntry entry = new HttpSinkRequestEntry("POST", new byte[] {1});
        HttpClient httpClient = mock(HttpClient.class);
        IOException failure = new IOException("connection refused");
        doReturn(CompletableFuture.failedFuture(failure)).when(httpClient).sendAsync(any(), any());
        RequestSubmitter submitter =
                recordingSubmitterFactory(httpClient, batchMode, new CopyOnWriteArrayList<>())
                        .createSubmitter(sinkConfig(batchProperties("0")), new String[0]);

        try {
            JavaNetHttpResponseWrapper outcome =
                    submitter.submit("http://localhost", List.of(entry)).get(0).join();

            assertThat(outcome.getResponse()).isEmpty();
            assertThat(outcome.getFailure()).containsSame(failure);
            assertThat(outcome.getRequestDurationMillis()).isGreaterThanOrEqualTo(0);
        } finally {
            submitter.close();
        }
    }

    private static HttpPostRequestCallback<HttpRequest> recordingCallback(
            List<List<HttpSinkRequestEntry>> httpRequests) {
        return (response, request, endpointUrl, headerMap) ->
                httpRequests.add(request.getRequestEntries());
    }

    private static List<HttpSinkRequestEntry> batchEntries() {
        return List.of(
                new HttpSinkRequestEntry("POST", new byte[] {1}),
                new HttpSinkRequestEntry("POST", new byte[] {2}),
                new HttpSinkRequestEntry("POST", new byte[] {3}));
    }

    private static Properties batchProperties(String maxRetries) {
        Properties properties = new Properties();
        properties.setProperty(HttpConnectorConfigConstants.SINK_HTTP_BATCH_REQUEST_SIZE, "50");
        properties.setProperty(SINK_MAX_RETRIES, maxRetries);
        properties.setProperty(SINK_RETRY_FIXED_DELAY_DELAY, "1ms");
        return properties;
    }

    private static RequestSubmitterFactory recordingSubmitterFactory(
            HttpClient httpClient, boolean batchMode, List<List<HttpSinkRequestEntry>> attempts) {
        return (sinkConfig, headersAndValues) -> {
            RequestSubmitter delegate =
                    batchMode
                            ? new BatchRequestSubmitter(
                                    sinkConfig,
                                    headersAndValues,
                                    httpClient,
                                    Executors.newSingleThreadExecutor())
                            : new PerRequestSubmitter(
                                    sinkConfig,
                                    headersAndValues,
                                    httpClient,
                                    Executors.newSingleThreadExecutor());
            return new RequestSubmitter() {
                @Override
                public List<CompletableFuture<JavaNetHttpResponseWrapper>> submit(
                        String endpointUrl, List<HttpSinkRequestEntry> requestToSubmit) {
                    attempts.add(List.copyOf(requestToSubmit));
                    return delegate.submit(endpointUrl, requestToSubmit);
                }

                @Override
                public void close() {
                    delegate.close();
                }
            };
        };
    }

    private static CompletableFuture<HttpResponse<String>> httpResponse(int statusCode) {
        HttpResponse<String> response = mock(HttpResponse.class);
        when(response.statusCode()).thenReturn(statusCode);
        return CompletableFuture.completedFuture(response);
    }

    private static void assertSameEntries(
            List<HttpSinkRequestEntry> actual, HttpSinkRequestEntry... expected) {
        assertThat(actual)
                .usingElementComparator((left, right) -> left == right ? 0 : 1)
                .containsExactly(expected);
    }

    private static void assertSameEntriesInAnyOrder(
            List<HttpSinkRequestEntry> actual, HttpSinkRequestEntry... expected) {
        assertThat(actual)
                .usingElementComparator((left, right) -> left == right ? 0 : 1)
                .containsExactlyInAnyOrder(expected);
    }

    private static CompletableFuture<JavaNetHttpResponseWrapper> responseFuture(
            HttpSinkRequestEntry requestEntry, int statusCode) {
        HttpResponse<String> response = mock(HttpResponse.class);
        when(response.statusCode()).thenReturn(statusCode);
        return CompletableFuture.completedFuture(
                new JavaNetHttpResponseWrapper(httpRequest(requestEntry), response));
    }

    private static CompletableFuture<JavaNetHttpResponseWrapper> responseFuture(
            HttpSinkRequestEntry requestEntry, int statusCode, long requestDurationMillis) {
        HttpResponse<String> response = mock(HttpResponse.class);
        when(response.statusCode()).thenReturn(statusCode);
        return CompletableFuture.completedFuture(
                new JavaNetHttpResponseWrapper(
                        httpRequest(requestEntry), response, requestDurationMillis));
    }

    private static CompletableFuture<JavaNetHttpResponseWrapper> responseFuture(
            List<HttpSinkRequestEntry> requestEntries, int statusCode, long requestDurationMillis) {
        HttpSinkRequestEntry first = requestEntries.get(0);
        HttpRequest request =
                new HttpRequest(
                        java.net.http.HttpRequest.newBuilder(
                                        java.net.URI.create("http://localhost"))
                                .method(
                                        first.method,
                                        java.net.http.HttpRequest.BodyPublishers.noBody())
                                .build(),
                        List.of(first.element),
                        first.method,
                        requestEntries);
        HttpResponse<String> response = mock(HttpResponse.class);
        when(response.statusCode()).thenReturn(statusCode);
        return CompletableFuture.completedFuture(
                new JavaNetHttpResponseWrapper(request, response, requestDurationMillis));
    }

    /** A request that failed before any response arrived, as the submitters report it. */
    private static CompletableFuture<JavaNetHttpResponseWrapper> responseFutureWithoutResponse(
            HttpSinkRequestEntry requestEntry) {
        return CompletableFuture.completedFuture(
                new JavaNetHttpResponseWrapper(httpRequest(requestEntry), null));
    }

    private static CompletableFuture<JavaNetHttpResponseWrapper> responseFutureWithoutStatus(
            HttpSinkRequestEntry requestEntry) {
        return CompletableFuture.completedFuture(
                new JavaNetHttpResponseWrapper(
                        httpRequest(requestEntry), mock(HttpResponse.class)));
    }

    private static HttpRequest httpRequest(HttpSinkRequestEntry requestEntry) {
        return new HttpRequest(
                java.net.http.HttpRequest.newBuilder(java.net.URI.create("http://localhost"))
                        .method(
                                requestEntry.method,
                                java.net.http.HttpRequest.BodyPublishers.noBody())
                        .build(),
                List.of(requestEntry.element),
                requestEntry.method,
                List.of(requestEntry));
    }
}
