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

package org.apache.flink.connector.http.sink.httpclient;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.connector.http.HttpLogger;
import org.apache.flink.connector.http.HttpPostRequestCallback;
import org.apache.flink.connector.http.clients.SinkHttpClient;
import org.apache.flink.connector.http.clients.SinkHttpClientBuilder;
import org.apache.flink.connector.http.clients.SinkHttpClientContext;
import org.apache.flink.connector.http.clients.SinkHttpClientResponses;
import org.apache.flink.connector.http.config.HttpSinkConfig;
import org.apache.flink.connector.http.config.SinkRequestSubmitMode;
import org.apache.flink.connector.http.preprocessor.HeaderPreprocessor;
import org.apache.flink.connector.http.retry.SinkRetryConfigProvider;
import org.apache.flink.connector.http.sink.HttpSinkRequestEntry;
import org.apache.flink.connector.http.utils.HttpHeaderUtils;

import lombok.extern.slf4j.Slf4j;

import java.net.http.HttpClient;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

/**
 * An implementation of {@link SinkHttpClient} that uses Java 11's {@link HttpClient}. This
 * implementation supports HTTP traffic only.
 */
@Slf4j
public class JavaNetSinkHttpClient implements SinkHttpClient {

    private final String[] headersAndValues;

    private final Map<String, String> headerMap;

    private final HttpSinkResponseClassifier responseClassifier;

    private final HttpPostRequestCallback<HttpRequest> httpPostRequestCallback;

    private final RequestSubmitter requestSubmitter;

    private final HttpLogger httpLogger;

    private final HttpSinkClientWithRetry httpClientWithRetry;

    public static SinkHttpClientBuilder builder() {
        return new SinkHttpClientBuilder() {
            @Override
            public SinkHttpClient build(SinkHttpClientContext context) {
                return new JavaNetSinkHttpClient(
                        context.getSinkConfig(),
                        context.getHeaderPreprocessor(),
                        createRequestSubmitterFactory(context));
            }
        };
    }

    JavaNetSinkHttpClient(
            HttpSinkConfig sinkConfig,
            HeaderPreprocessor headerPreprocessor,
            RequestSubmitterFactory requestSubmitterFactory) {

        this.httpPostRequestCallback = sinkConfig.getHttpPostRequestCallback();
        this.headerMap = sinkConfig.prepareHeaderMap(headerPreprocessor);

        this.responseClassifier = new HttpSinkResponseClassifier(sinkConfig);

        this.headersAndValues = HttpHeaderUtils.toHeaderAndValueArray(this.headerMap);
        this.requestSubmitter =
                requestSubmitterFactory.createSubmitter(sinkConfig, headersAndValues);

        this.httpLogger = HttpLogger.getHttpLogger(sinkConfig.getProperties());
        this.httpClientWithRetry =
                new HttpSinkClientWithRetry(SinkRetryConfigProvider.create(sinkConfig));
    }

    @Override
    public CompletableFuture<SinkHttpClientResponses> putRequests(
            List<HttpSinkRequestEntry> requestEntries, String endpointUrl) {
        return httpClientWithRetry.send(
                requestEntries, requestsToSubmit -> submitAttempt(requestsToSubmit, endpointUrl));
    }

    @Override
    public void close() {
        try {
            requestSubmitter.close();
        } finally {
            httpClientWithRetry.close();
        }
    }

    private CompletionStage<HttpSinkAttemptResult> submitAttempt(
            List<HttpSinkRequestEntry> requestEntries, String endpointUrl) {
        return submitRequests(requestEntries, endpointUrl)
                .thenApply(responses -> prepareAttemptResult(responses, endpointUrl));
    }

    private CompletableFuture<List<JavaNetHttpResponseWrapper>> submitRequests(
            List<HttpSinkRequestEntry> requestEntries, String endpointUrl) {

        var responseFutures = requestSubmitter.submit(endpointUrl, requestEntries);
        var allFutures = CompletableFuture.allOf(responseFutures.toArray(new CompletableFuture[0]));
        return allFutures.thenApply(
                _void ->
                        responseFutures.stream()
                                .map(CompletableFuture::join)
                                .collect(Collectors.toList()));
    }

    private HttpSinkAttemptResult prepareAttemptResult(
            List<JavaNetHttpResponseWrapper> responses, String endpointUrl) {
        var attemptResult = new HttpSinkAttemptResult();

        for (var response : responses) {
            var sinkRequestEntry = response.getHttpRequest();
            var optResponse = response.getResponse();
            optResponse.ifPresent(this.httpLogger::logResponse);
            httpPostRequestCallback.call(
                    optResponse.orElse(null), sinkRequestEntry, endpointUrl, headerMap);

            switch (responseClassifier.classify(optResponse.orElse(null))) {
                case SUCCESS:
                    attemptResult.addSuccessfulRequests(sinkRequestEntry.getRequestEntries());
                    break;
                case IGNORED:
                    attemptResult.addIgnoredRequests(sinkRequestEntry.getRequestEntries());
                    break;
                case RETRYABLE_FAILURE:
                    attemptResult.addRetryableRequests(sinkRequestEntry.getRequestEntries());
                    break;
                case FATAL_FAILURE:
                    attemptResult.addFatalFailedRequests(sinkRequestEntry.getRequestEntries());
                    break;
            }
        }

        return attemptResult;
    }

    @VisibleForTesting
    String[] getHeadersAndValues() {
        return Arrays.copyOf(headersAndValues, headersAndValues.length);
    }

    private static RequestSubmitterFactory createRequestSubmitterFactory(
            SinkHttpClientContext context) {
        String requestMode = context.getSinkConfig().getRequestMode();
        if (SinkRequestSubmitMode.SINGLE.getMode().equalsIgnoreCase(requestMode)) {
            return new PerRequestRequestSubmitterFactory();
        }
        return new BatchRequestSubmitterFactory(context.getDefaultBatchSize());
    }
}
