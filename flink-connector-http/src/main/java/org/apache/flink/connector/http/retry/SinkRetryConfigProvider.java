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

package org.apache.flink.connector.http.retry;

import org.apache.flink.connector.http.config.HttpSinkConfig;

import io.github.resilience4j.core.IntervalFunction;
import io.github.resilience4j.retry.RetryConfig;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;

import static io.github.resilience4j.core.IntervalFunction.ofExponentialBackoff;

/** Configuration for HTTP sink retry. */
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class SinkRetryConfigProvider {

    private final HttpSinkConfig sinkConfig;

    public static RetryConfig create(HttpSinkConfig sinkConfig) {
        return new SinkRetryConfigProvider(sinkConfig).create();
    }

    private RetryConfig create() {
        if (sinkConfig.getMaxRetries() == 0) {
            return RetryConfig.custom().maxAttempts(1).build();
        }
        return createBuilder().maxAttempts(sinkConfig.getMaxRetries() + 1).build();
    }

    private RetryConfig.Builder<?> createBuilder() {
        var retryStrategy = RetryStrategyType.fromCode(sinkConfig.getRetryStrategy());
        if (retryStrategy == RetryStrategyType.FIXED_DELAY) {
            return RetryConfig.custom()
                    .intervalFunction(IntervalFunction.of(sinkConfig.getRetryFixedDelay()));
        } else if (retryStrategy == RetryStrategyType.EXPONENTIAL_DELAY) {
            return RetryConfig.custom()
                    .intervalFunction(
                            ofExponentialBackoff(
                                    sinkConfig.getRetryExponentialDelayInitialBackoff(),
                                    sinkConfig.getRetryExponentialDelayMultiplier(),
                                    sinkConfig.getRetryExponentialDelayMaxBackoff()));
        }
        throw new IllegalArgumentException("Unsupported retry strategy: " + retryStrategy);
    }
}
