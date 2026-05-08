/* Licensed to the Apache Software Foundation (ASF) under one or more
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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.source.lookup.LookupOptions;

import io.github.resilience4j.core.IntervalFunction;
import io.github.resilience4j.retry.RetryConfig;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;

import java.time.Duration;

import static io.github.resilience4j.core.IntervalFunction.ofExponentialBackoff;
import static org.apache.flink.connector.http.table.lookup.HttpLookupConnectorOptions.SOURCE_LOOKUP_RETRY_EXPONENTIAL_DELAY_INITIAL_BACKOFF;
import static org.apache.flink.connector.http.table.lookup.HttpLookupConnectorOptions.SOURCE_LOOKUP_RETRY_EXPONENTIAL_DELAY_MAX_BACKOFF;
import static org.apache.flink.connector.http.table.lookup.HttpLookupConnectorOptions.SOURCE_LOOKUP_RETRY_EXPONENTIAL_DELAY_MULTIPLIER;
import static org.apache.flink.connector.http.table.lookup.HttpLookupConnectorOptions.SOURCE_LOOKUP_RETRY_FIXED_DELAY_DELAY;
import static org.apache.flink.connector.http.table.lookup.HttpLookupConnectorOptions.SOURCE_LOOKUP_RETRY_STRATEGY;

/**
 * Configuration for Retry.
 *
 * <p>The provider is generic: it works for the lookup source (via {@link #create(ReadableConfig)})
 * and for any other component (e.g. the HTTP sink) by passing a custom set of {@link ConfigOption}s
 * through {@link #create(ReadableConfig, RetryOptionKeys)}.
 */
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class RetryConfigProvider {

    private final ReadableConfig config;
    private final RetryOptionKeys keys;
    private final int maxAttempts;

    /** Create a {@link RetryConfig} using the lookup-source defaults and {@code max-retries}. */
    public static RetryConfig create(ReadableConfig config) {
        return new RetryConfigProvider(
                        config,
                        RetryOptionKeys.lookupSource(),
                        config.get(LookupOptions.MAX_RETRIES) + 1)
                .build();
    }

    /**
     * Create a {@link RetryConfig} for a component providing its own option keys and a pre-computed
     * max-attempts value (max-retries + 1).
     */
    public static RetryConfig create(ReadableConfig config, RetryOptionKeys keys, int maxAttempts) {
        return new RetryConfigProvider(config, keys, maxAttempts).build();
    }

    /**
     * Build the {@link IntervalFunction} alone — handy for components that drive their own retry
     * loop (e.g. {@code HttpSinkWriter}) but still want to share the fixed-delay /
     * exponential-delay behaviour with the lookup source.
     */
    public static IntervalFunction intervalFunction(ReadableConfig config, RetryOptionKeys keys) {
        return new RetryConfigProvider(config, keys, 1).buildIntervalFunction();
    }

    private RetryConfig build() {
        return createBuilder().maxAttempts(maxAttempts).build();
    }

    private RetryConfig.Builder<?> createBuilder() {
        return RetryConfig.custom().intervalFunction(buildIntervalFunction());
    }

    private IntervalFunction buildIntervalFunction() {
        var retryStrategy = RetryStrategyType.fromCode(config.get(keys.strategy()));
        if (retryStrategy == RetryStrategyType.FIXED_DELAY) {
            return IntervalFunction.of(config.get(keys.fixedDelay()));
        } else if (retryStrategy == RetryStrategyType.EXPONENTIAL_DELAY) {
            Duration initialDelay = config.get(keys.exponentialInitialBackoff());
            Duration maxDelay = config.get(keys.exponentialMaxBackoff());
            double multiplier = config.get(keys.exponentialMultiplier());
            return ofExponentialBackoff(initialDelay, multiplier, maxDelay);
        }
        throw new IllegalArgumentException("Unsupported retry strategy: " + retryStrategy);
    }

    /** Bag of {@link ConfigOption} references identifying the retry-related options. */
    public static final class RetryOptionKeys {

        private final ConfigOption<String> strategy;
        private final ConfigOption<Duration> fixedDelay;
        private final ConfigOption<Duration> exponentialInitialBackoff;
        private final ConfigOption<Duration> exponentialMaxBackoff;
        private final ConfigOption<Double> exponentialMultiplier;

        public RetryOptionKeys(
                ConfigOption<String> strategy,
                ConfigOption<Duration> fixedDelay,
                ConfigOption<Duration> exponentialInitialBackoff,
                ConfigOption<Duration> exponentialMaxBackoff,
                ConfigOption<Double> exponentialMultiplier) {
            this.strategy = strategy;
            this.fixedDelay = fixedDelay;
            this.exponentialInitialBackoff = exponentialInitialBackoff;
            this.exponentialMaxBackoff = exponentialMaxBackoff;
            this.exponentialMultiplier = exponentialMultiplier;
        }

        static RetryOptionKeys lookupSource() {
            return new RetryOptionKeys(
                    SOURCE_LOOKUP_RETRY_STRATEGY,
                    SOURCE_LOOKUP_RETRY_FIXED_DELAY_DELAY,
                    SOURCE_LOOKUP_RETRY_EXPONENTIAL_DELAY_INITIAL_BACKOFF,
                    SOURCE_LOOKUP_RETRY_EXPONENTIAL_DELAY_MAX_BACKOFF,
                    SOURCE_LOOKUP_RETRY_EXPONENTIAL_DELAY_MULTIPLIER);
        }

        ConfigOption<String> strategy() {
            return strategy;
        }

        ConfigOption<Duration> fixedDelay() {
            return fixedDelay;
        }

        ConfigOption<Duration> exponentialInitialBackoff() {
            return exponentialInitialBackoff;
        }

        ConfigOption<Duration> exponentialMaxBackoff() {
            return exponentialMaxBackoff;
        }

        ConfigOption<Double> exponentialMultiplier() {
            return exponentialMultiplier;
        }
    }
}
