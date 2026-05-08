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

import org.apache.flink.connector.http.sink.HttpSinkRequestEntry;
import org.apache.flink.connector.http.sink.httpclient.HttpRequest;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Data class holding {@link HttpSinkRequestEntry} instances that {@link SinkHttpClient} attempted
 * to write, divided into successful, retryable (transient HTTP failures) and fatal failures.
 *
 * <p>Retryable failures are requests whose response status code matches the configured {@code
 * retry-codes} (by default {@code 500,503,504}). Fatal failures are everything else that is not a
 * success (4xx, non-listed 5xx, etc). The HTTP sink will only replay retryable failures; fatal
 * failures are counted as errors immediately without blocking the pipeline.
 */
@Getter
@ToString
@EqualsAndHashCode
public class SinkHttpClientResponse {

    /** A list of successfully written requests. */
    @NonNull private final List<HttpRequest> successfulRequests;

    /** Requests that failed with a transient HTTP status code and may be retried. */
    @NonNull private final List<HttpRequest> retryableFailedRequests;

    /** Requests that failed with a non-retryable status code (fatal failures). */
    @NonNull private final List<HttpRequest> fatalFailedRequests;

    public SinkHttpClientResponse(
            @NonNull List<HttpRequest> successfulRequests,
            @NonNull List<HttpRequest> retryableFailedRequests,
            @NonNull List<HttpRequest> fatalFailedRequests) {
        this.successfulRequests = successfulRequests;
        this.retryableFailedRequests = retryableFailedRequests;
        this.fatalFailedRequests = fatalFailedRequests;
    }

    /**
     * Backwards compatible constructor: every failed request is considered retryable. Provided so
     * existing callers / tests keep working.
     */
    public SinkHttpClientResponse(
            @NonNull List<HttpRequest> successfulRequests,
            @NonNull List<HttpRequest> failedRequests) {
        this(successfulRequests, failedRequests, Collections.emptyList());
    }

    /**
     * All failed requests, regardless of whether they are retryable or fatal. Kept for backwards
     * compatibility with code written before the retryable/fatal split was introduced.
     */
    public List<HttpRequest> getFailedRequests() {
        List<HttpRequest> all =
                new ArrayList<>(retryableFailedRequests.size() + fatalFailedRequests.size());
        all.addAll(retryableFailedRequests);
        all.addAll(fatalFailedRequests);
        return all;
    }
}
