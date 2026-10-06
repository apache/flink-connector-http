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

package org.apache.flink.connector.http.clients;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.connector.http.sink.HttpSinkRequestEntry;
import org.apache.flink.connector.http.sink.httpclient.HttpRequest;

import lombok.Data;
import lombok.NonNull;
import lombok.ToString;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Data class holding {@link HttpSinkRequestEntry} instances that {@link SinkHttpClient} attempted
 * to write, divided by the outcome of their HTTP response.
 */
@Data
@PublicEvolving
@ToString
public class SinkHttpClientResponses {

    /** A list of successfully written requests. */
    @NonNull private final List<HttpSinkRequestEntry> successfulRequests;

    /** A list of requests that {@link SinkHttpClient} failed with a retryable failure. */
    @NonNull private final List<HttpSinkRequestEntry> retriableFailedRequests;

    /** A list of requests that {@link SinkHttpClient} failed with a fatal failure. */
    @NonNull private final List<HttpSinkRequestEntry> fatalFailedRequests;

    /**
     * A list of requests whose response status code was configured as ignored. They are neither
     * retried nor treated as failures.
     */
    @NonNull private final List<HttpSinkRequestEntry> ignoredRequests;

    public SinkHttpClientResponses(
            List<HttpSinkRequestEntry> successfulRequests,
            List<HttpSinkRequestEntry> retriableFailedRequests,
            List<HttpSinkRequestEntry> fatalFailedRequests,
            List<HttpSinkRequestEntry> ignoredRequests) {
        this.successfulRequests = successfulRequests;
        this.retriableFailedRequests = retriableFailedRequests;
        this.fatalFailedRequests = fatalFailedRequests;
        this.ignoredRequests = ignoredRequests;
    }

    public SinkHttpClientResponses(
            List<HttpSinkRequestEntry> successfulRequests,
            List<HttpSinkRequestEntry> retriableFailedRequests,
            List<HttpSinkRequestEntry> fatalFailedRequests) {
        this(
                successfulRequests,
                retriableFailedRequests,
                fatalFailedRequests,
                Collections.emptyList());
    }

    /**
     * Compatibility constructor for custom clients built against the previous response shape, which
     * grouped requests as {@link HttpRequest}. The given requests are flattened into their {@link
     * HttpSinkRequestEntry} elements.
     *
     * @deprecated Use {@link #SinkHttpClientResponses(List, List, List, List)} instead. Note that
     *     {@link #getSuccessfulRequests()} and {@link #getRetriableFailedRequests()} now return
     *     {@link HttpSinkRequestEntry} elements instead of {@link HttpRequest}, so custom clients
     *     that read these lists must be updated and recompiled.
     */
    @Deprecated
    public SinkHttpClientResponses(
            List<HttpRequest> successfulRequests, List<HttpRequest> failedRequests) {
        this(
                flatten(successfulRequests),
                flatten(failedRequests),
                Collections.emptyList(),
                Collections.emptyList());
    }

    /**
     * @deprecated Use {@link #getRetriableFailedRequests()} instead.
     */
    @Deprecated
    public List<HttpSinkRequestEntry> getFailedRequests() {
        return retriableFailedRequests;
    }

    private static List<HttpSinkRequestEntry> flatten(List<HttpRequest> requests) {
        return requests.stream()
                .flatMap(request -> request.getRequestEntries().stream())
                .collect(Collectors.toList());
    }
}
