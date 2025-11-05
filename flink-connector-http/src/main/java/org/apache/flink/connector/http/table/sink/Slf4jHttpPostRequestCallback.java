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

package org.apache.flink.connector.http.table.sink;

import org.apache.flink.connector.http.HttpPostRequestCallback;
import org.apache.flink.connector.http.sink.httpclient.HttpRequest;

import lombok.extern.slf4j.Slf4j;

import java.net.http.HttpResponse;
import java.util.Map;

/**
 * A {@link HttpPostRequestCallback} that logs pairs of request and response as <i>INFO</i> level
 * logs using <i>Slf4j</i>.
 *
 * <p>Serving as a default implementation of {@link HttpPostRequestCallback} for the {@link
 * HttpDynamicSink}.
 */
@Slf4j
public class Slf4jHttpPostRequestCallback implements HttpPostRequestCallback<HttpRequest> {

    @Override
    public void call(
            HttpResponse<String> response,
            HttpRequest requestEntry,
            String endpointUrl,
            Map<String, String> headerMap) {

        // Uncomment if you want to see the requestBody in the log
        // String requestBody = requestEntry.getElements().stream()
        //    .map(element -> new String(element, StandardCharsets.UTF_8))
        //    .collect(Collectors.joining());

        if (response == null) {
            log.info(
                    "Got response for a request.\n  Request:\n    "
                            + "Method: {}\n   Response: null",
                    requestEntry.getMethod());
        } else {
            log.info(
                    "Got response for a request.\n  Request:\n    "
                            + "Method: {}\n  Response status code: {}\n ",
                    requestEntry.method,
                    response.statusCode());
        }
    }
}
