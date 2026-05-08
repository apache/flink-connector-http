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
import org.apache.flink.connector.http.clients.SinkHttpClientResponse;
import org.apache.flink.connector.http.config.HttpConnectorConfigConstants;
import org.apache.flink.connector.http.preprocessor.HeaderPreprocessor;
import org.apache.flink.connector.http.sink.HttpSinkRequestEntry;
import org.apache.flink.connector.http.status.ComposeHttpStatusCodeChecker;
import org.apache.flink.connector.http.status.ComposeHttpStatusCodeChecker.ComposeHttpStatusCodeCheckerConfig;
import org.apache.flink.connector.http.status.HttpCodesParser;
import org.apache.flink.connector.http.status.HttpResponseChecker;
import org.apache.flink.connector.http.status.HttpStatusCodeChecker;
import org.apache.flink.connector.http.utils.HttpHeaderUtils;
import org.apache.flink.util.ConfigurationException;

import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nullable;

import java.net.http.HttpClient;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * An implementation of {@link SinkHttpClient} that uses Java 11's {@link HttpClient}. This
 * implementation supports HTTP traffic only.
 */
@Slf4j
public class JavaNetSinkHttpClient implements SinkHttpClient {

    private final String[] headersAndValues;

    private final Map<String, String> headerMap;

    private final HttpStatusCodeChecker statusCodeChecker;

    /**
     * Optional retry-aware response checker. Populated only when the user supplies {@code
     * http.sink.retry-codes} or {@code http.sink.success-codes}; when {@code null} the client falls
     * back to the legacy {@link #statusCodeChecker} and marks every failure as retryable,
     * preserving the historical sink behaviour.
     */
    @Nullable private final HttpResponseChecker retryAwareResponseChecker;

    private final HttpPostRequestCallback<HttpRequest> httpPostRequestCallback;

    private final RequestSubmitter requestSubmitter;

    private final HttpLogger httpLogger;

    public JavaNetSinkHttpClient(
            Properties properties,
            HttpPostRequestCallback<HttpRequest> httpPostRequestCallback,
            HeaderPreprocessor headerPreprocessor,
            RequestSubmitterFactory requestSubmitterFactory) {

        this.httpPostRequestCallback = httpPostRequestCallback;
        this.headerMap =
                HttpHeaderUtils.prepareHeaderMap(
                        HttpConnectorConfigConstants.SINK_HEADER_PREFIX,
                        properties,
                        headerPreprocessor);

        // TODO Inject this via constructor when implementing a response processor.
        //  Processor will be injected and it will wrap statusChecker implementation.
        ComposeHttpStatusCodeCheckerConfig checkerConfig =
                ComposeHttpStatusCodeCheckerConfig.builder()
                        .properties(properties)
                        .includeListPrefix(
                                HttpConnectorConfigConstants.HTTP_ERROR_SINK_CODE_INCLUDE_LIST)
                        .errorCodePrefix(HttpConnectorConfigConstants.HTTP_ERROR_SINK_CODES_LIST)
                        .build();

        this.statusCodeChecker = new ComposeHttpStatusCodeChecker(checkerConfig);
        this.retryAwareResponseChecker = buildRetryAwareResponseChecker(properties);

        this.headersAndValues = HttpHeaderUtils.toHeaderAndValueArray(this.headerMap);
        this.requestSubmitter =
                requestSubmitterFactory.createSubmitter(properties, headersAndValues);

        this.httpLogger = HttpLogger.getHttpLogger(properties);
    }

    @Nullable
    private static HttpResponseChecker buildRetryAwareResponseChecker(Properties properties) {
        String successCodes =
                properties.getProperty(HttpConnectorConfigConstants.SINK_RETRY_SUCCESS_CODES);
        String retryCodes =
                properties.getProperty(HttpConnectorConfigConstants.SINK_RETRY_RETRY_CODES);
        if (successCodes == null && retryCodes == null) {
            // Neither option was set: keep the legacy behaviour (one-bucket failures) so existing
            // users are unaffected.
            return null;
        }
        try {
            return new HttpResponseChecker(
                    HttpCodesParser.parse(successCodes != null ? successCodes : "2XX"),
                    HttpCodesParser.parse(retryCodes != null ? retryCodes : "500,503,504"));
        } catch (ConfigurationException e) {
            throw new IllegalArgumentException(
                    "Invalid HTTP sink success/retry codes configuration: " + e.getMessage(), e);
        }
    }

    @Override
    public CompletableFuture<SinkHttpClientResponse> putRequests(
            List<HttpSinkRequestEntry> requestEntries, String endpointUrl) {
        return submitRequests(requestEntries, endpointUrl)
                .thenApply(responses -> prepareSinkHttpClientResponse(responses, endpointUrl));
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

    private SinkHttpClientResponse prepareSinkHttpClientResponse(
            List<JavaNetHttpResponseWrapper> responses, String endpointUrl) {
        var successfulResponses = new ArrayList<HttpRequest>();
        var retryableFailedResponses = new ArrayList<HttpRequest>();
        var fatalFailedResponses = new ArrayList<HttpRequest>();

        for (var response : responses) {
            var sinkRequestEntry = response.getHttpRequest();
            var optResponse = response.getResponse();
            this.httpLogger.logResponse(response.getResponse().get());
            httpPostRequestCallback.call(
                    optResponse.orElse(null), sinkRequestEntry, endpointUrl, headerMap);

            if (optResponse.isEmpty()) {
                // Network-level failure (e.g. IOException) — treat as retryable regardless of the
                // active checker so callers can drive their own retry policy.
                retryableFailedResponses.add(sinkRequestEntry);
                continue;
            }

            int statusCode = optResponse.get().statusCode();
            if (retryAwareResponseChecker != null) {
                if (retryAwareResponseChecker.isSuccessful(statusCode)) {
                    successfulResponses.add(sinkRequestEntry);
                } else if (retryAwareResponseChecker.isTemporalError(statusCode)) {
                    retryableFailedResponses.add(sinkRequestEntry);
                } else {
                    fatalFailedResponses.add(sinkRequestEntry);
                }
            } else if (statusCodeChecker.isErrorCode(statusCode)) {
                // Legacy behaviour: everything not marked as success is considered retryable so
                // existing users keep the pre-retry-split semantics.
                retryableFailedResponses.add(sinkRequestEntry);
            } else {
                successfulResponses.add(sinkRequestEntry);
            }
        }

        return new SinkHttpClientResponse(
                successfulResponses, retryableFailedResponses, fatalFailedResponses);
    }

    @VisibleForTesting
    String[] getHeadersAndValues() {
        return Arrays.copyOf(headersAndValues, headersAndValues.length);
    }
}
