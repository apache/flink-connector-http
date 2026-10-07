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

import org.apache.flink.connector.http.config.HttpConnectorConfigConstants;
import org.apache.flink.connector.http.config.HttpSinkConfig;
import org.apache.flink.connector.http.config.HttpSinkConfigFactory;
import org.apache.flink.connector.http.status.ComposeHttpStatusCodeChecker;
import org.apache.flink.connector.http.status.ComposeHttpStatusCodeChecker.ComposeHttpStatusCodeCheckerConfig;
import org.apache.flink.connector.http.status.HttpCodesParser;
import org.apache.flink.connector.http.status.HttpResponseChecker;
import org.apache.flink.util.ConfigurationException;
import org.apache.flink.util.StringUtils;

import lombok.extern.slf4j.Slf4j;

import java.net.http.HttpResponse;
import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.connector.http.table.sink.HttpDynamicSinkConnectorOptions.SINK_HTTP_RETRY_CODES;
import static org.apache.flink.connector.http.table.sink.HttpDynamicSinkConnectorOptions.SINK_HTTP_SUCCESS_CODES;

/** Maps HTTP sink responses to a status using sink status-code configuration. */
@Slf4j
public class HttpSinkResponseClassifier {

    private final Set<Integer> ignoredResponseCodes;
    private final HttpResponseChecker responseChecker;
    private final ComposeHttpStatusCodeChecker legacyResponseChecker;

    public HttpSinkResponseClassifier(HttpSinkConfig sinkConfig) {
        // Safety net for hand-built configs; Table/DataStream already validate in the factory.
        // Ignored-vs-exclude uses properties only, so factory-mapped exclude values do not fail
        // here.
        HttpSinkConfigFactory.validateLegacyAndNewStatusCodeOptionsAreExclusive(
                sinkConfig.getReadableConfig(), sinkConfig.getProperties());
        try {
            ignoredResponseCodes = HttpCodesParser.parse(sinkConfig.getIgnoredResponseCodes());
            legacyResponseChecker = createLegacyResponseChecker(sinkConfig);
            var successCodes = new HashSet<Integer>();
            successCodes.addAll(HttpCodesParser.parse(sinkConfig.getSuccessCodes()));
            successCodes.addAll(ignoredResponseCodes);
            var retryCodes = new HashSet<>(HttpCodesParser.parse(sinkConfig.getRetryCodes()));
            retryCodes.removeAll(ignoredResponseCodes);
            responseChecker = new HttpResponseChecker(successCodes, retryCodes);
        } catch (ConfigurationException e) {
            throw new IllegalArgumentException("Invalid HTTP sink status-code configuration", e);
        }
    }

    public HttpSinkResponseStatus classify(HttpResponse<?> response) {
        if (response == null) {
            return HttpSinkResponseStatus.RETRYABLE_FAILURE;
        }
        if (ignoredResponseCodes.contains(response.statusCode())) {
            return HttpSinkResponseStatus.IGNORED;
        }
        if (legacyResponseChecker != null) {
            return legacyResponseChecker.isErrorCode(response.statusCode())
                    ? HttpSinkResponseStatus.FATAL_FAILURE
                    : HttpSinkResponseStatus.SUCCESS;
        }
        if (responseChecker.isSuccessful(response)) {
            return HttpSinkResponseStatus.SUCCESS;
        }
        if (responseChecker.isTemporalError(response)) {
            return HttpSinkResponseStatus.RETRYABLE_FAILURE;
        }
        return HttpSinkResponseStatus.FATAL_FAILURE;
    }

    private ComposeHttpStatusCodeChecker createLegacyResponseChecker(HttpSinkConfig sinkConfig) {
        boolean hasLegacyErrorCodes =
                !StringUtils.isNullOrWhitespaceOnly(
                        sinkConfig
                                .getProperties()
                                .getProperty(
                                        HttpConnectorConfigConstants.HTTP_ERROR_SINK_CODES_LIST));
        boolean hasLegacyExcludedCodes =
                !StringUtils.isNullOrWhitespaceOnly(
                        sinkConfig
                                .getProperties()
                                .getProperty(
                                        HttpConnectorConfigConstants
                                                .HTTP_ERROR_SINK_CODE_INCLUDE_LIST));
        if (!hasLegacyErrorCodes && !hasLegacyExcludedCodes) {
            return null;
        }

        log.warn(
                "Legacy HTTP sink error-code properties are set ({} / {}). "
                        + "These remain supported; prefer {} and {} for new jobs.",
                HttpConnectorConfigConstants.HTTP_ERROR_SINK_CODES_LIST,
                HttpConnectorConfigConstants.HTTP_ERROR_SINK_CODE_INCLUDE_LIST,
                SINK_HTTP_SUCCESS_CODES.key(),
                SINK_HTTP_RETRY_CODES.key());

        return new ComposeHttpStatusCodeChecker(
                ComposeHttpStatusCodeCheckerConfig.builder()
                        .properties(sinkConfig.getProperties())
                        .includeListPrefix(
                                HttpConnectorConfigConstants.HTTP_ERROR_SINK_CODE_INCLUDE_LIST)
                        .errorCodePrefix(HttpConnectorConfigConstants.HTTP_ERROR_SINK_CODES_LIST)
                        .build());
    }
}
