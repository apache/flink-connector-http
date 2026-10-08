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

package org.apache.flink.connector.http.preprocessor;

import org.apache.flink.connector.http.auth.OidcAccessTokenManager;

import lombok.extern.slf4j.Slf4j;

import java.net.http.HttpClient;
import java.time.Duration;
import java.util.Optional;

/** Header processor for HTTP OIDC Authentication mechanism. */
@Slf4j
public class OIDCAuthHeaderValuePreprocessor implements HeaderValuePreprocessor {

    private final String oidcAuthURL;
    private final String oidcTokenRequest;
    private Duration oidcExpiryReduction =
            OidcAccessTokenManager.DEFAULT_TOKEN_EXPIRATION_REDUCTION;
    private final Duration tokenRequestTimeout;

    /**
     * Created on first use and kept for the life of this preprocessor instance, so every request
     * served by the same instance reuses the cached token until it expires. The field is transient:
     * after deserialization it is created again and a new token is fetched on the next request.
     */
    private transient OidcAccessTokenManager accessTokenManager;

    /**
     * Add the access token to the request using OidcAuth authenticate method that gives us a valid
     * access token.
     *
     * @param oidcAuthURL OIDC token endpoint
     * @param oidcTokenRequest OIDC Token Request
     * @param oidcExpiryReduction OIDC token expiry reduction
     */
    public OIDCAuthHeaderValuePreprocessor(
            String oidcAuthURL, String oidcTokenRequest, Optional<Duration> oidcExpiryReduction) {
        this(
                oidcAuthURL,
                oidcTokenRequest,
                oidcExpiryReduction,
                OidcAccessTokenManager.DEFAULT_TOKEN_REQUEST_TIMEOUT);
    }

    /**
     * Add the access token to the request using OidcAuth authenticate method that gives us a valid
     * access token.
     *
     * @param oidcAuthURL OIDC token endpoint
     * @param oidcTokenRequest OIDC Token Request
     * @param oidcExpiryReduction OIDC token expiry reduction
     * @param tokenRequestTimeout how long one call to the token endpoint may take before it fails
     */
    public OIDCAuthHeaderValuePreprocessor(
            String oidcAuthURL,
            String oidcTokenRequest,
            Optional<Duration> oidcExpiryReduction,
            Duration tokenRequestTimeout) {
        this.oidcAuthURL = oidcAuthURL;
        this.oidcTokenRequest = oidcTokenRequest;
        if (oidcExpiryReduction.isPresent()) {
            this.oidcExpiryReduction = oidcExpiryReduction.get();
        }
        this.tokenRequestTimeout = tokenRequestTimeout;
    }

    /**
     * Returns the bearer header value, reusing the cached access token until it expires.
     * Synchronized because the token manager is not thread-safe and lookups can run concurrently.
     */
    @Override
    public synchronized String preprocessHeaderValue(String rawValue) {
        if (accessTokenManager == null) {
            accessTokenManager =
                    new OidcAccessTokenManager(
                            HttpClient.newBuilder().connectTimeout(tokenRequestTimeout).build(),
                            oidcTokenRequest,
                            oidcAuthURL,
                            oidcExpiryReduction,
                            tokenRequestTimeout);
        }
        // apply the OIDC authentication by adding the dynamically calculated header value.
        return "Bearer " + accessTokenManager.authenticate();
    }
}
