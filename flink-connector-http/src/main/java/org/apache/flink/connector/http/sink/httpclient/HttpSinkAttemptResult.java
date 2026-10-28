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

import org.apache.flink.connector.http.sink.HttpSinkRequestEntry;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Result of one HTTP sink client submission attempt. */
class HttpSinkAttemptResult {

    private final List<HttpSinkRequestEntry> successfulRequests = new ArrayList<>();

    private final List<HttpSinkRequestEntry> retryableRequests = new ArrayList<>();

    private final List<HttpSinkRequestEntry> fatalFailedRequests = new ArrayList<>();

    private final List<HttpSinkRequestEntry> ignoredRequests = new ArrayList<>();

    private final Map<Integer, Integer> statusCodeCounts = new HashMap<>();

    private final List<Long> requestLatenciesMillis = new ArrayList<>();

    private int httpRequestCount;

    private int transportFailedEntries;

    void addSuccessfulRequests(List<HttpSinkRequestEntry> requestEntries) {
        successfulRequests.addAll(requestEntries);
    }

    void addIgnoredRequests(List<HttpSinkRequestEntry> requestEntries) {
        ignoredRequests.addAll(requestEntries);
    }

    void addRetryableRequests(List<HttpSinkRequestEntry> requestEntries) {
        retryableRequests.addAll(requestEntries);
    }

    void addFatalFailedRequests(List<HttpSinkRequestEntry> requestEntries) {
        fatalFailedRequests.addAll(requestEntries);
    }

    /** Records one completed HTTP request; a negative duration means it was not measured. */
    void recordHttpRequest(long requestDurationMillis) {
        httpRequestCount++;
        if (requestDurationMillis >= 0) {
            requestLatenciesMillis.add(requestDurationMillis);
        }
    }

    void recordStatusCode(int statusCode, int requestEntryCount) {
        statusCodeCounts.merge(statusCode, requestEntryCount, Integer::sum);
    }

    /** Records request entries whose HTTP request failed before a response was received. */
    void recordTransportFailure(int requestEntryCount) {
        transportFailedEntries += requestEntryCount;
    }

    List<HttpSinkRequestEntry> getSuccessfulRequests() {
        return successfulRequests;
    }

    List<HttpSinkRequestEntry> getRetryableRequests() {
        return retryableRequests;
    }

    List<HttpSinkRequestEntry> getFatalFailedRequests() {
        return fatalFailedRequests;
    }

    List<HttpSinkRequestEntry> getIgnoredRequests() {
        return ignoredRequests;
    }

    Map<Integer, Integer> getStatusCodeCounts() {
        return statusCodeCounts;
    }

    List<Long> getRequestLatenciesMillis() {
        return requestLatenciesMillis;
    }

    int getHttpRequestCount() {
        return httpRequestCount;
    }

    int getTransportFailedEntries() {
        return transportFailedEntries;
    }

    boolean hasRetryableRequests() {
        return !retryableRequests.isEmpty();
    }
}
