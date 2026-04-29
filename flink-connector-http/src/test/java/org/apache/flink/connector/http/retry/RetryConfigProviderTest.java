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

package org.apache.flink.connector.http.retry;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.connector.source.lookup.LookupOptions;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.stream.IntStream;

import static org.apache.flink.connector.http.table.lookup.HttpLookupConnectorOptions.SOURCE_LOOKUP_RETRY_EXPONENTIAL_DELAY_INITIAL_BACKOFF;
import static org.apache.flink.connector.http.table.lookup.HttpLookupConnectorOptions.SOURCE_LOOKUP_RETRY_EXPONENTIAL_DELAY_MAX_BACKOFF;
import static org.apache.flink.connector.http.table.lookup.HttpLookupConnectorOptions.SOURCE_LOOKUP_RETRY_EXPONENTIAL_DELAY_MULTIPLIER;
import static org.apache.flink.connector.http.table.lookup.HttpLookupConnectorOptions.SOURCE_LOOKUP_RETRY_FIXED_DELAY_DELAY;
import static org.apache.flink.connector.http.table.lookup.HttpLookupConnectorOptions.SOURCE_LOOKUP_RETRY_STRATEGY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;

/** Test for {@link RetryConfigProvider}. */
class RetryConfigProviderTest {

    @Test
    void verifyFixedDelayRetryConfig() {
        var config = new Configuration();
        config.set(SOURCE_LOOKUP_RETRY_STRATEGY, "fixed-delay");
        config.set(SOURCE_LOOKUP_RETRY_FIXED_DELAY_DELAY, Duration.ofSeconds(10));
        config.set(LookupOptions.MAX_RETRIES, 12);
        var retryConfig = RetryConfigProvider.create(config);

        assertThat(retryConfig.getMaxAttempts()).isEqualTo(13);
        IntStream.range(1, 12)
                .forEach(
                        attempt ->
                                assertThat(retryConfig.getIntervalFunction().apply(attempt))
                                        .isEqualTo(10000));
    }

    @Test
    void verifyExponentialDelayConfig() {
        var config = new Configuration();
        config.set(SOURCE_LOOKUP_RETRY_STRATEGY, "exponential-delay");

        config.set(SOURCE_LOOKUP_RETRY_EXPONENTIAL_DELAY_INITIAL_BACKOFF, Duration.ofMillis(15));
        config.set(SOURCE_LOOKUP_RETRY_EXPONENTIAL_DELAY_MAX_BACKOFF, Duration.ofMillis(120));
        config.set(SOURCE_LOOKUP_RETRY_EXPONENTIAL_DELAY_MULTIPLIER, 2.0);
        config.set(LookupOptions.MAX_RETRIES, 6);

        var retryConfig = RetryConfigProvider.create(config);
        var intervalFunction = retryConfig.getIntervalFunction();

        assertThat(retryConfig.getMaxAttempts()).isEqualTo(7);
        assertThat(intervalFunction.apply(1)).isEqualTo(15);
        assertThat(intervalFunction.apply(2)).isEqualTo(30);
        assertThat(intervalFunction.apply(3)).isEqualTo(60);
        assertThat(intervalFunction.apply(4)).isEqualTo(120);
        assertThat(intervalFunction.apply(5)).isEqualTo(120);
        assertThat(intervalFunction.apply(6)).isEqualTo(120);
    }

    @Test
    void failWhenStrategyIsUnsupported() {
        var config = new Configuration();
        config.set(SOURCE_LOOKUP_RETRY_STRATEGY, "dummy");

        try (var mockedStatic = mockStatic(RetryStrategyType.class)) {
            var dummyStrategy = mock(RetryStrategyType.class);
            mockedStatic.when(() -> RetryStrategyType.fromCode("dummy")).thenReturn(dummyStrategy);

            assertThatThrownBy(() -> RetryConfigProvider.create(config))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }
}
