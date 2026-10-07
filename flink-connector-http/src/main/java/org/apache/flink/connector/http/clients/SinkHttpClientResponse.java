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

import java.util.List;

/**
 * @deprecated Use {@link SinkHttpClientResponses} instead.
 */
@Deprecated
@PublicEvolving
public class SinkHttpClientResponse extends SinkHttpClientResponses {

    public SinkHttpClientResponse(
            List<HttpSinkRequestEntry> successfulRequests,
            List<HttpSinkRequestEntry> retriableFailedRequests,
            List<HttpSinkRequestEntry> fatalFailedRequests,
            List<HttpSinkRequestEntry> ignoredRequests) {
        super(successfulRequests, retriableFailedRequests, fatalFailedRequests, ignoredRequests);
    }

    public SinkHttpClientResponse(
            List<HttpSinkRequestEntry> successfulRequests,
            List<HttpSinkRequestEntry> retriableFailedRequests,
            List<HttpSinkRequestEntry> fatalFailedRequests) {
        super(successfulRequests, retriableFailedRequests, fatalFailedRequests);
    }

    /**
     * @deprecated Use {@link SinkHttpClientResponses#SinkHttpClientResponses(List, List)} instead.
     */
    @Deprecated
    public SinkHttpClientResponse(
            List<HttpRequest> successfulRequests, List<HttpRequest> failedRequests) {
        super(successfulRequests, failedRequests);
    }
}
