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

package org.apache.flink.connector.http;

import org.apache.flink.connector.http.config.HttpConnectorConfigConstants;

import lombok.Data;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpRequest;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/** Test class for logging HTTP requests. */
public class HttpLoggerRequestTest {

    @ParameterizedTest
    @MethodSource("configProvider")
    void testCreateStringForRequest(TestSpec testSpec) throws URISyntaxException {

        Properties properties = new Properties();
        if (testSpec.httpLoggingLevelType != null) {
            properties.put(
                    HttpConnectorConfigConstants.HTTP_LOGGING_LEVEL,
                    testSpec.getHttpLoggingLevelType().name());
        }

        HttpLogger httpLogger = HttpLogger.getHttpLogger(properties);
        URI uri = new URI("http://aaa");
        HttpRequest.Builder httpRequestBuilder = HttpRequest.newBuilder().uri(uri);
        if (testSpec.isHasHeaders()) {
            httpRequestBuilder.headers("bbb", "ccc", "bbb", "ddd", "eee", "fff");
        }
        if (testSpec.getMethod().equals("POST")) {
            if (testSpec.isHasBody()) {
                httpRequestBuilder.method("POST", HttpRequest.BodyPublishers.ofString("my body"));
            } else {
                httpRequestBuilder.method("POST", HttpRequest.BodyPublishers.noBody());
            }
        }
        assertThat(httpLogger.createStringForRequest(httpRequestBuilder.build()))
                .isEqualTo(testSpec.getExpectedOutput());
    }

    @Data
    static class TestSpec {
        final HttpLoggingLevelType httpLoggingLevelType;
        final String method;
        final boolean hasBody;
        final boolean hasHeaders;
        final String expectedOutput;
    }

    static Collection<TestSpec> configProvider() {
        return List.of(
                // GET no headers
                new TestSpec(
                        null,
                        "GET",
                        false,
                        false,
                        "HTTP GET Request: URL: http://aaa, Headers: None"),
                new TestSpec(
                        HttpLoggingLevelType.MIN,
                        "GET",
                        false,
                        false,
                        "HTTP GET Request: URL: http://aaa, Headers: None"),
                new TestSpec(
                        HttpLoggingLevelType.REQ_RESP,
                        "GET",
                        false,
                        false,
                        "HTTP GET Request: URL: http://aaa, Headers: None"),
                new TestSpec(
                        HttpLoggingLevelType.MAX,
                        "GET",
                        false,
                        false,
                        "HTTP GET Request: URL: http://aaa, Headers: None"),
                // GET with headers
                new TestSpec(
                        null,
                        "GET",
                        false,
                        true,
                        "HTTP GET Request: URL: http://aaa, Headers: ***"),
                new TestSpec(
                        HttpLoggingLevelType.MIN,
                        "GET",
                        false,
                        true,
                        "HTTP GET Request: URL: http://aaa, Headers: ***"),
                new TestSpec(
                        HttpLoggingLevelType.REQ_RESP,
                        "GET",
                        false,
                        true,
                        "HTTP GET Request: URL: http://aaa, Headers: ***"),
                new TestSpec(
                        HttpLoggingLevelType.MAX,
                        "GET",
                        false,
                        true,
                        "HTTP GET Request: URL: http://aaa, Headers: bbb:[ccc;ddd];eee:[fff]"),

                // POST no headers
                new TestSpec(
                        null,
                        "POST",
                        false,
                        false,
                        "HTTP POST Request: URL: http://aaa, Headers: None"),
                new TestSpec(
                        HttpLoggingLevelType.MIN,
                        "POST",
                        false,
                        false,
                        "HTTP POST Request: URL: http://aaa, Headers: None"),
                new TestSpec(
                        HttpLoggingLevelType.REQ_RESP,
                        "POST",
                        false,
                        false,
                        "HTTP POST Request: URL: http://aaa, Headers: None"),
                new TestSpec(
                        HttpLoggingLevelType.MAX,
                        "POST",
                        false,
                        false,
                        "HTTP POST Request: URL: http://aaa, Headers: None"),
                // POST with headers
                new TestSpec(
                        null,
                        "POST",
                        false,
                        true,
                        "HTTP POST Request: URL: http://aaa, Headers: ***"),
                new TestSpec(
                        HttpLoggingLevelType.MIN,
                        "POST",
                        false,
                        true,
                        "HTTP POST Request: URL: http://aaa, Headers: ***"),
                new TestSpec(
                        HttpLoggingLevelType.REQ_RESP,
                        "POST",
                        false,
                        true,
                        "HTTP POST Request: URL: http://aaa, Headers: ***"),
                new TestSpec(
                        HttpLoggingLevelType.MAX,
                        "POST",
                        false,
                        true,
                        "HTTP POST Request: URL: http://aaa, Headers: bbb:[ccc;ddd];eee:[fff]"),

                // POST no headers with body
                new TestSpec(
                        null,
                        "POST",
                        true,
                        false,
                        "HTTP POST Request: URL: http://aaa, Headers: None"),
                new TestSpec(
                        HttpLoggingLevelType.MIN,
                        "POST",
                        true,
                        false,
                        "HTTP POST Request: URL: http://aaa, Headers: None"),
                new TestSpec(
                        HttpLoggingLevelType.REQ_RESP,
                        "POST",
                        true,
                        false,
                        "HTTP POST Request: URL: http://aaa, Headers: None"),
                new TestSpec(
                        HttpLoggingLevelType.MAX,
                        "POST",
                        true,
                        false,
                        "HTTP POST Request: URL: http://aaa, Headers: None"),
                // POST with headers with body
                new TestSpec(
                        null,
                        "POST",
                        true,
                        true,
                        "HTTP POST Request: URL: http://aaa, Headers: ***"),
                new TestSpec(
                        HttpLoggingLevelType.MIN,
                        "POST",
                        true,
                        true,
                        "HTTP POST Request: URL: http://aaa, Headers: ***"),
                new TestSpec(
                        HttpLoggingLevelType.REQ_RESP,
                        "POST",
                        true,
                        true,
                        "HTTP POST Request: URL: http://aaa, Headers: ***"),
                new TestSpec(
                        HttpLoggingLevelType.MAX,
                        "POST",
                        true,
                        true,
                        "HTTP POST Request: URL: http://aaa, Headers: bbb:[ccc;ddd];eee:[fff]"));
    }
}
