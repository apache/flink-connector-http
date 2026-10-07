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
import org.apache.flink.connector.http.config.HttpConnectorConfigConstants;
import org.apache.flink.connector.http.config.HttpSinkConfig;
import org.apache.flink.connector.http.config.HttpSinkConfigFactory;
import org.apache.flink.connector.http.preprocessor.HeaderPreprocessor;
import org.apache.flink.connector.http.sink.HttpSinkRequestEntry;
import org.apache.flink.connector.http.table.sink.Slf4jHttpPostRequestCallback;
import org.apache.flink.connector.http.utils.HttpHeaderUtils;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

import java.net.http.HttpClient;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.apache.flink.connector.http.TestHelper.assertPropertyArray;
import static org.apache.flink.connector.http.config.HttpConnectorConfigConstants.SINK_MAX_RETRIES;
import static org.apache.flink.connector.http.config.HttpConnectorConfigConstants.SINK_RETRY_FIXED_DELAY_DELAY;
import static org.apache.flink.connector.http.table.sink.HttpDynamicSinkConnectorOptions.SINK_HTTP_IGNORED_RESPONSE_CODES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
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

    private static CompletableFuture<JavaNetHttpResponseWrapper> responseFuture(
            HttpSinkRequestEntry requestEntry, int statusCode) {
        HttpResponse<String> response = mock(HttpResponse.class);
        when(response.statusCode()).thenReturn(statusCode);
        return CompletableFuture.completedFuture(
                new JavaNetHttpResponseWrapper(httpRequest(requestEntry), response));
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
