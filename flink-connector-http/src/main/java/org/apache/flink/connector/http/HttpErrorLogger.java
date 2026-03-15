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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.http;

import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.net.http.HttpHeaders;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Map;
import java.util.Properties;

import static org.apache.flink.connector.http.config.HttpConnectorConfigConstants.ERROR_LOG_BODY_MAX_SIZE;
import static org.apache.flink.connector.http.config.HttpConnectorConfigConstants.ERROR_LOG_INCLUDE_BODY;
import static org.apache.flink.connector.http.config.HttpConnectorConfigConstants.ERROR_LOG_LEVEL;

/**
 * Enhanced HTTP error logger with configurable verbosity levels. This logger provides detailed
 * error context including request details, response information (when available), and retry
 * attempts.
 */
@Slf4j
public class HttpErrorLogger implements Serializable {

    private static final long serialVersionUID = 1L;

    private final HttpErrorLogLevel errorLogLevel;
    private final boolean includeBody;
    private final int maxBodySize;

    private HttpErrorLogger(Properties properties) {
        this.errorLogLevel =
                HttpErrorLogLevel.fromString(properties.getProperty(ERROR_LOG_LEVEL, "ERROR"));
        this.includeBody =
                Boolean.parseBoolean(properties.getProperty(ERROR_LOG_INCLUDE_BODY, "false"));
        this.maxBodySize =
                Integer.parseInt(properties.getProperty(ERROR_LOG_BODY_MAX_SIZE, "1024"));
    }

    public static HttpErrorLogger getLogger(Properties properties) {
        return new HttpErrorLogger(properties);
    }

    /**
     * Log HTTP lookup source error with detailed context.
     *
     * @param request The HTTP request
     * @param e The exception that occurred
     * @param retryAttempt The retry attempt number (0 for first attempt)
     */
    public void logLookupError(HttpRequest request, Exception e, int retryAttempt) {
        String message =
                String.format(
                        "HTTP Lookup Error - Attempt %d: Method: %s, URL: %s, Exception: %s, Message: %s",
                        retryAttempt,
                        request.method(),
                        request.uri(),
                        e.getClass().getSimpleName(),
                        e.getMessage());

        logWithLevel(message);
    }

    /**
     * Log HTTP lookup error with response details.
     *
     * @param request The HTTP request
     * @param response The HTTP response (may be null if error occurred before response)
     * @param e The exception that occurred
     * @param retryAttempt The retry attempt number
     */
    public void logLookupError(
            HttpRequest request, HttpResponse<?> response, Exception e, int retryAttempt) {
        StringBuilder message =
                new StringBuilder(
                        String.format(
                                "HTTP Lookup Error - Attempt %d: Method: %s, URL: %s, Exception: %s, Message: %s",
                                retryAttempt,
                                request.method(),
                                request.uri(),
                                e.getClass().getSimpleName(),
                                e.getMessage()));

        // Add response details if available
        if (response != null) {
            message.append(
                    String.format(
                            ", Response Status: %d, Response Headers: %s",
                            response.statusCode(), getHeadersForLog(response.headers())));

            // Add response body if configured
            if (includeBody && response.body() != null) {
                String body = response.body().toString();
                message.append(String.format(", Response Body: %s", truncateBody(body)));
            }
        }

        // Add request headers
        message.append(String.format(", Request Headers: %s", getHeadersForLog(request.headers())));

        logWithLevel(message.toString());
    }

    /**
     * Log HTTP sink error with detailed context.
     *
     * @param request The HTTP request
     * @param requestBody The request body (may be null)
     * @param e The exception that occurred
     * @param retryAttempt The retry attempt number
     */
    public void logSinkError(
            HttpRequest request, String requestBody, Exception e, int retryAttempt) {
        StringBuilder message =
                new StringBuilder(
                        String.format(
                                "HTTP Sink Error - Attempt %d: Method: %s, URL: %s, Exception: %s, Message: %s",
                                retryAttempt,
                                request.method(),
                                request.uri(),
                                e.getClass().getSimpleName(),
                                e.getMessage()));

        // Add request body if configured
        if (includeBody && requestBody != null) {
            message.append(String.format(", Request Body: %s", truncateBody(requestBody)));
        }

        // Add request headers
        message.append(String.format(", Request Headers: %s", getHeadersForLog(request.headers())));

        logWithLevel(message.toString());
    }

    /**
     * Log HTTP sink error with response details.
     *
     * @param request The HTTP request
     * @param requestBody The request body (may be null)
     * @param response The HTTP response
     * @param e The exception that occurred
     * @param retryAttempt The retry attempt number
     */
    public void logSinkError(
            HttpRequest request,
            String requestBody,
            HttpResponse<String> response,
            Exception e,
            int retryAttempt) {
        StringBuilder message =
                new StringBuilder(
                        String.format(
                                "HTTP Sink Error - Attempt %d: Method: %s, URL: %s, Exception: %s, Message: %s",
                                retryAttempt,
                                request.method(),
                                request.uri(),
                                e.getClass().getSimpleName(),
                                e.getMessage()));

        // Add response details
        message.append(
                String.format(
                        ", Response Status: %d, Response Headers: %s",
                        response.statusCode(), getHeadersForLog(response.headers())));

        // Add response body if configured
        if (includeBody && response.body() != null) {
            message.append(String.format(", Response Body: %s", truncateBody(response.body())));
        }

        // Add request body if configured
        if (includeBody && requestBody != null) {
            message.append(String.format(", Request Body: %s", truncateBody(requestBody)));
        }

        // Add request headers
        message.append(String.format(", Request Headers: %s", getHeadersForLog(request.headers())));

        logWithLevel(message.toString());
    }

    /**
     * Log HTTP error without request context.
     *
     * @param url The URL that caused the error
     * @param e The exception that occurred
     */
    public void logError(String url, Exception e) {
        String message =
                String.format(
                        "HTTP Error - URL: %s, Exception: %s, Message: %s",
                        url, e.getClass().getSimpleName(), e.getMessage());
        logWithLevel(message);
    }

    private String getHeadersForLog(HttpHeaders headers) {
        if (headers == null) {
            return "None";
        }
        Map<String, java.util.List<String>> headersMap = headers.map();
        if (headersMap.isEmpty()) {
            return "None";
        }

        // Filter out sensitive headers
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (Map.Entry<String, java.util.List<String>> entry : headersMap.entrySet()) {
            String headerName = entry.getKey().toLowerCase();
            // Mask sensitive headers
            if (headerName.contains("authorization")
                    || headerName.contains("cookie")
                    || headerName.contains("api-key")
                    || headerName.contains("x-api-key")) {
                if (!first) {
                    sb.append("; ");
                }
                sb.append(entry.getKey()).append(": ***");
                first = false;
            } else {
                if (!first) {
                    sb.append("; ");
                }
                sb.append(entry.getKey()).append(": ").append(entry.getValue());
                first = false;
            }
        }
        return sb.toString();
    }

    private String truncateBody(String body) {
        if (body == null || body.isEmpty()) {
            return "None";
        }
        if (body.length() <= maxBodySize) {
            return body;
        }
        return body.substring(0, maxBodySize) + "... (truncated)";
    }

    private void logWithLevel(String message) {
        switch (errorLogLevel) {
            case ERROR:
                log.error(message);
                break;
            case WARN:
                log.warn(message);
                break;
            case INFO:
                log.info(message);
                break;
            default:
                log.error(message);
                break;
        }
    }
}
