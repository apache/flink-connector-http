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

import org.apache.flink.connector.http.table.lookup.HttpLookupSourceRequestEntry;

import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.net.http.HttpHeaders;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.StringJoiner;

import static org.apache.flink.connector.http.config.HttpConnectorConfigConstants.HTTP_LOGGING_LEVEL;

/**
 * HttpLogger, this is a class to perform HTTP content logging based on a level defined in
 * configuration.
 */
@Slf4j
public class HttpLogger implements Serializable {

    private static final long serialVersionUID = 1L;

    private final HttpLoggingLevelType httpLoggingLevelType;

    private HttpLogger(Properties properties) {
        String code = (String) properties.get(HTTP_LOGGING_LEVEL);
        this.httpLoggingLevelType = HttpLoggingLevelType.valueOfStr(code);
    }

    public static HttpLogger getHttpLogger(Properties properties) {
        return new HttpLogger(properties);
    }

    public void logRequest(HttpRequest httpRequest) {
        if (log.isDebugEnabled()) {
            log.debug(createStringForRequest(httpRequest));
        }
    }

    public void logResponse(HttpResponse<String> response) {

        if (log.isDebugEnabled()) {
            log.debug(createStringForResponse(response));
        }
    }

    public void logRequestBody(String body) {
        if (log.isDebugEnabled()) {
            log.debug(createStringForBody(body));
        }
    }

    public void logExceptionResponse(HttpLookupSourceRequestEntry request, Exception e) {
        if (log.isDebugEnabled()) {
            log.debug(createStringForExceptionResponse(request, e));
        }
    }

    String createStringForRequest(HttpRequest httpRequest) {
        String headersForLog = getHeadersForLog(httpRequest.headers());
        return String.format(
                "HTTP %s Request: URL: %s, Headers: %s",
                httpRequest.method(), httpRequest.uri().toString(), headersForLog);
    }

    private String getHeadersForLog(HttpHeaders httpHeaders) {
        if (httpHeaders == null) {
            return "None";
        }
        Map<String, List<String>> headersMap = httpHeaders.map();
        if (headersMap.isEmpty()) {
            return "None";
        }
        if (this.httpLoggingLevelType == HttpLoggingLevelType.MAX) {
            StringJoiner headers = new StringJoiner(";");
            for (Map.Entry<String, List<String>> reqHeaders : headersMap.entrySet()) {
                StringJoiner values = new StringJoiner(";");
                for (String value : reqHeaders.getValue()) {
                    values.add(value);
                }
                String header = reqHeaders.getKey() + ":[" + values + "]";
                headers.add(header);
            }
            return headers.toString();
        }
        return "***";
    }

    String createStringForResponse(HttpResponse<String> response) {
        String headersForLog = getHeadersForLog(response.headers());

        String bodyForLog = "***";
        if (response.body() == null || response.body().isEmpty()) {
            bodyForLog = "None";
        } else {
            if (this.httpLoggingLevelType != HttpLoggingLevelType.MIN) {
                bodyForLog = response.body().toString();
            }
        }
        return String.format(
                "HTTP %s Response: URL: %s,"
                        + " Response Headers: %s, status code: %s, Response Body: %s",
                response.request().method(),
                response.uri(),
                headersForLog,
                response.statusCode(),
                bodyForLog);
    }

    private String createStringForExceptionResponse(
            HttpLookupSourceRequestEntry request, Exception e) {
        HttpRequest httpRequest = request.getHttpRequest();
        return String.format(
                "HTTP %s Exception Response: URL: %s Exception %s",
                httpRequest.method(), httpRequest.uri(), e);
    }

    String createStringForBody(String body) {
        String bodyForLog = "***";
        if (body == null || body.isEmpty()) {
            bodyForLog = "None";
        } else {
            if (this.httpLoggingLevelType != HttpLoggingLevelType.MIN) {
                bodyForLog = body.toString();
            }
        }
        return bodyForLog;
    }
}
