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

import static org.apache.flink.connector.http.config.HttpConnectorConfigConstants.ERROR_LOG_SEVERITY;
import static org.apache.flink.connector.http.config.HttpConnectorConfigConstants.HTTP_LOGGING_LEVEL;

/**
 * HttpLogger, this is a class to perform HTTP content logging based on a level defined in
 * configuration. It also handles error logging with configurable severity levels.
 */
@Slf4j
public class HttpLogger implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final int DEFAULT_MAX_BODY_SIZE = 1024;

    private final HttpLoggingLevelType httpLoggingLevelType;
    private final HttpErrorLogSeverity errorLogSeverity;

    private HttpLogger(Properties properties) {
        String code = (String) properties.get(HTTP_LOGGING_LEVEL);
        this.httpLoggingLevelType = HttpLoggingLevelType.valueOfStr(code);
        
        String severityStr = (String) properties.get(ERROR_LOG_SEVERITY);
        this.errorLogSeverity = HttpErrorLogSeverity.fromString(severityStr);
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

    /**
     * Log HTTP lookup error with detailed context.
     *
     * @param request The HTTP request
     * @param e The exception that occurred
     * @param retryAttempt The retry attempt number (0 for first attempt)
     * @param continueOnError Whether the error is tolerated (continue-on-error mode)
     */
    public void logLookupError(HttpRequest request, Exception e, int retryAttempt, boolean continueOnError) {
        // If continue-on-error is true, always use DEBUG (error is tolerated)
        if (continueOnError) {
            if (log.isDebugEnabled()) {
                log.debug(
                    "HTTP Lookup Error (tolerated) - Attempt {}: Method: {}, URL: {}, Exception: {}, Message: {}",
                    retryAttempt, request.method(), request.uri(), e.getClass().getSimpleName(), e.getMessage());
            }
            return;
        }

        // For actual errors, respect http.error.log.severity
        String message = formatLookupErrorMessage(request, null, e, retryAttempt);
        logWithSeverity(message);
    }

    /**
     * Log HTTP lookup error with response details.
     *
     * @param request The HTTP request
     * @param response The HTTP response (may be null if error occurred before response)
     * @param e The exception that occurred
     * @param retryAttempt The retry attempt number
     * @param continueOnError Whether the error is tolerated (continue-on-error mode)
     */
    public void logLookupError(
            HttpRequest request,
            HttpResponse<?> response,
            Exception e,
            int retryAttempt,
            boolean continueOnError) {
        
        // If continue-on-error is true, always use DEBUG (error is tolerated)
        if (continueOnError) {
            if (log.isDebugEnabled()) {
                log.debug(
                    "HTTP Lookup Error (tolerated) - Attempt {}: Method: {}, URL: {}, Exception: {}, Message: {}, Response Status: {}",
                    retryAttempt, request.method(), request.uri(), e.getClass().getSimpleName(), 
                    e.getMessage(), response != null ? response.statusCode() : "N/A");
            }
            return;
        }

        // For actual errors, respect http.error.log.severity
        String message = formatLookupErrorMessage(request, response, e, retryAttempt);
        logWithSeverity(message);
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
            HttpRequest request,
            String requestBody,
            Exception e,
            int retryAttempt) {
        String message = formatSinkErrorMessage(request, requestBody, null, e, retryAttempt);
        logWithSeverity(message);
    }

    /**
     * Log HTTP sink error for non-successful status code responses.
     *
     * @param request The HTTP request
     * @param requestBody The request body (may be null)
     * @param response The HTTP response with error status code
     * @param retryAttempt The retry attempt number
     */
    public void logSinkError(
            HttpRequest request,
            String requestBody,
            HttpResponse<String> response,
            int retryAttempt) {
        String message = formatSinkErrorMessage(request, requestBody, response, null, retryAttempt);
        logWithSeverity(message);
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
        String message = formatSinkErrorMessage(request, requestBody, response, e, retryAttempt);
        logWithSeverity(message);
    }

    private String formatLookupErrorMessage(
            HttpRequest request,
            HttpResponse<?> response,
            Exception e,
            int retryAttempt) {
        StringBuilder message = new StringBuilder(
            String.format(
                "HTTP Lookup Error - Attempt %d: Method: %s, URL: %s, Exception: %s, Message: %s",
                retryAttempt,
                request.method(),
                request.uri(),
                e.getClass().getSimpleName(),
                e.getMessage()));

        // Add response details if available (based on logging level)
        if (response != null) {
            message.append(String.format(", Response Status: %d", response.statusCode()));
            
            if (httpLoggingLevelType != HttpLoggingLevelType.MIN) {
                message.append(String.format(", Response Headers: %s", getHeadersForLog(response.headers())));
                
                if (httpLoggingLevelType == HttpLoggingLevelType.MAX && response.body() != null) {
                    message.append(String.format(", Response Body: %s", truncateBody(response.body().toString())));
                }
            }
        }

        // Add request headers based on logging level
        if (httpLoggingLevelType != HttpLoggingLevelType.MIN) {
            message.append(String.format(", Request Headers: %s", getHeadersForLog(request.headers())));
        }

        return message.toString();
    }

    private String formatSinkErrorMessage(
            HttpRequest request,
            String requestBody,
            HttpResponse<String> response,
            Exception e,
            int retryAttempt) {
        StringBuilder message = new StringBuilder();
        
        if (e != null) {
            message.append(String.format(
                "HTTP Sink Error - Attempt %d: Method: %s, URL: %s, Exception: %s, Message: %s",
                retryAttempt,
                request.method(),
                request.uri(),
                e.getClass().getSimpleName(),
                e.getMessage()));
        } else {
            message.append(String.format(
                "HTTP Sink Error - Attempt %d: Method: %s, URL: %s",
                retryAttempt,
                request.method(),
                request.uri()));
        }

        // Add response details if available (based on logging level)
        if (response != null) {
            message.append(String.format(", Response Status: %d", response.statusCode()));
            
            if (httpLoggingLevelType != HttpLoggingLevelType.MIN) {
                message.append(String.format(", Response Headers: %s", getHeadersForLog(response.headers())));
                
                if (httpLoggingLevelType == HttpLoggingLevelType.MAX && response.body() != null) {
                    message.append(String.format(", Response Body: %s", truncateBody(response.body())));
                }
            }
        }

        // Add request body based on logging level
        if (httpLoggingLevelType == HttpLoggingLevelType.MAX && requestBody != null) {
            message.append(String.format(", Request Body: %s", truncateBody(requestBody)));
        }

        // Add request headers based on logging level
        if (httpLoggingLevelType != HttpLoggingLevelType.MIN) {
            message.append(String.format(", Request Headers: %s", getHeadersForLog(request.headers())));
        }

        return message.toString();
    }

    private void logWithSeverity(String message) {
        switch (errorLogSeverity) {
            case ERROR:
                log.error(message);
                break;
            case WARN:
                log.warn(message);
                break;
            case INFO:
                log.info(message);
                break;
            case OFF:
                // No error-level logging, fall back to DEBUG
                if (log.isDebugEnabled()) {
                    log.debug(message);
                }
                break;
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
                String headerName = reqHeaders.getKey().toLowerCase();
                
                // Mask sensitive headers
                if (headerName.contains("authorization")
                        || headerName.contains("cookie")
                        || headerName.contains("api-key")
                        || headerName.contains("x-api-key")) {
                    headers.add(reqHeaders.getKey() + ":[***]");
                } else {
                    StringJoiner values = new StringJoiner(";");
                    for (String value : reqHeaders.getValue()) {
                        values.add(value);
                    }
                    String header = reqHeaders.getKey() + ":[" + values + "]";
                    headers.add(header);
                }
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

    private String truncateBody(String body) {
        if (body == null || body.isEmpty()) {
            return "None";
        }
        if (body.length() <= DEFAULT_MAX_BODY_SIZE) {
            return body;
        }
        return body.substring(0, DEFAULT_MAX_BODY_SIZE) + "... (truncated)";
    }
}
