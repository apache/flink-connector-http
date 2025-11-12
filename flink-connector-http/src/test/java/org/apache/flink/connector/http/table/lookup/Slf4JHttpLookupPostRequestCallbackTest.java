/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.http.table.lookup;

import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpRequest;

class Slf4JHttpLookupPostRequestCallbackTest {
    @Test
    public void testNullResponseDoesNotError() throws URISyntaxException {
        HttpRequest httpRequest =
                HttpRequest.newBuilder()
                        .method("GET", HttpRequest.BodyPublishers.ofString("foo"))
                        .uri(new URI("http://testing123"))
                        .build();
        HttpLookupSourceRequestEntry requestEntry =
                new HttpLookupSourceRequestEntry(httpRequest, new LookupQueryInfo(""));
        Slf4JHttpLookupPostRequestCallback slf4JHttpLookupPostRequestCallback =
                new Slf4JHttpLookupPostRequestCallback();
        slf4JHttpLookupPostRequestCallback.call(null, requestEntry, "aaa", null);
    }
}
