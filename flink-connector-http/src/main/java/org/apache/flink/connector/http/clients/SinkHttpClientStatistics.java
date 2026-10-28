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

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Observability data collected by a {@link SinkHttpClient} while writing one batch of {@link
 * HttpSinkRequestEntry} elements, across all retry attempts. The sink writer turns it into metrics.
 * Counts are in request entries unless stated otherwise. A custom client that does not collect this
 * data returns {@link #empty()}.
 */
@PublicEvolving
@Getter
@ToString
@EqualsAndHashCode
public class SinkHttpClientStatistics {

    private static final SinkHttpClientStatistics EMPTY =
            new SinkHttpClientStatistics(
                    0, Collections.emptyList(), Collections.emptyMap(), 0, 0, 0);

    /** Number of HTTP requests sent, including retries. */
    private final int httpRequestCount;

    /** Duration of each HTTP request in milliseconds, for requests where it was measured. */
    private final List<Long> requestLatenciesMillis;

    /** Number of request entries per HTTP response status code. */
    private final Map<Integer, Integer> statusCodeCounts;

    /** Request entries that received a retryable response status, counted on every attempt. */
    private final int retryableResponseEntries;

    /** Request entries whose HTTP request failed before a response was received. */
    private final int transportFailedEntries;

    /** Request entries that were scheduled for another attempt. */
    private final int retryAttemptEntries;

    public SinkHttpClientStatistics(
            int httpRequestCount,
            @NonNull List<Long> requestLatenciesMillis,
            @NonNull Map<Integer, Integer> statusCodeCounts,
            int retryableResponseEntries,
            int transportFailedEntries,
            int retryAttemptEntries) {
        this.httpRequestCount = httpRequestCount;
        this.requestLatenciesMillis = requestLatenciesMillis;
        this.statusCodeCounts = statusCodeCounts;
        this.retryableResponseEntries = retryableResponseEntries;
        this.transportFailedEntries = transportFailedEntries;
        this.retryAttemptEntries = retryAttemptEntries;
    }

    /** Statistics of a client that collected nothing. */
    public static SinkHttpClientStatistics empty() {
        return EMPTY;
    }
}
