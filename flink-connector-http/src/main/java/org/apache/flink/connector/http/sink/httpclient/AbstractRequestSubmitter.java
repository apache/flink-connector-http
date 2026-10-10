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

import org.apache.flink.connector.http.auth.OidcAccessTokenManager;
import org.apache.flink.connector.http.config.HttpSinkConfig;
import org.apache.flink.connector.http.utils.HttpHeaderUtils;
import org.apache.flink.connector.http.utils.ThreadUtils;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import java.net.http.HttpClient;
import java.net.http.HttpRequest.Builder;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/** Abstract request submitter. */
public abstract class AbstractRequestSubmitter implements RequestSubmitter {

    protected static final int HTTP_CLIENT_PUBLISHING_THREAD_POOL_SIZE = 1;

    /** Thread pool to handle HTTP response from HTTP client. */
    protected final ExecutorService publishingThreadPool;

    /** Thread pool used by the Java HTTP client for request work. */
    private final ExecutorService httpClientExecutor;

    protected final Duration httpRequestTimeout;

    protected final String[] headersAndValues;

    protected final HttpClient httpClient;

    protected final HttpClient.Version httpVersion;

    /** Supplies the OIDC bearer token for each request; {@code null} when OIDC is not set. */
    private final OidcAccessTokenManager oidcAccessTokenManager;

    public AbstractRequestSubmitter(
            HttpSinkConfig sinkConfig,
            String[] headersAndValues,
            HttpClient httpClient,
            ExecutorService httpClientExecutor) {

        this.headersAndValues = headersAndValues;
        this.httpClientExecutor = httpClientExecutor;
        this.publishingThreadPool =
                Executors.newFixedThreadPool(
                        HTTP_CLIENT_PUBLISHING_THREAD_POOL_SIZE,
                        new ExecutorThreadFactory(
                                "http-sink-client-response-worker",
                                ThreadUtils.LOGGING_EXCEPTION_HANDLER));

        this.httpRequestTimeout = sinkConfig.getRequestTimeout();
        this.httpVersion = HttpClient.Version.valueOf(sinkConfig.getHttpVersion());
        this.oidcAccessTokenManager =
                HttpHeaderUtils.createSinkOidcAccessTokenManager(sinkConfig.getReadableConfig());

        this.httpClient = httpClient;
    }

    @Override
    public void close() {
        publishingThreadPool.shutdownNow();
        httpClientExecutor.shutdownNow();
    }

    protected Builder newRequestBuilder() {
        return java.net.http.HttpRequest.newBuilder()
                .version(httpVersion)
                .timeout(httpRequestTimeout);
    }

    /**
     * Returns the headers for the next HTTP request. With OIDC configured, a current bearer token
     * replaces any configured {@code Authorization} header, so an expired token is refreshed before
     * the request is sent.
     */
    protected String[] requestHeaders() {
        if (oidcAccessTokenManager == null) {
            return headersAndValues;
        }

        String accessToken;
        // The token manager caches the token without synchronization, and requests are built
        // from both the writer thread and the retry scheduler.
        synchronized (oidcAccessTokenManager) {
            accessToken = oidcAccessTokenManager.authenticate();
        }

        List<String> headers = new ArrayList<>(headersAndValues.length + 2);
        for (int i = 0; i + 1 < headersAndValues.length; i += 2) {
            if (!HttpHeaderUtils.AUTHORIZATION.equalsIgnoreCase(headersAndValues[i])) {
                headers.add(headersAndValues[i]);
                headers.add(headersAndValues[i + 1]);
            }
        }
        headers.add(HttpHeaderUtils.AUTHORIZATION);
        headers.add("Bearer " + accessToken);
        return headers.toArray(new String[0]);
    }
}
