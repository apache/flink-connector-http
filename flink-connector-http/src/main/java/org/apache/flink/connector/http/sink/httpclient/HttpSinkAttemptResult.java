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
import java.util.List;

/** Result of one HTTP sink client submission attempt. */
class HttpSinkAttemptResult {

    private final List<HttpSinkRequestEntry> successfulRequests = new ArrayList<>();

    private final List<HttpSinkRequestEntry> retryableRequests = new ArrayList<>();

    private final List<HttpSinkRequestEntry> fatalFailedRequests = new ArrayList<>();

    private final List<HttpSinkRequestEntry> ignoredRequests = new ArrayList<>();

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

    boolean hasRetryableRequests() {
        return !retryableRequests.isEmpty();
    }
}
