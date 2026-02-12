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

import org.junit.jupiter.api.Test;

import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;

/** Test for {@link RetryConfigProvider}. */
class RetryConfigProviderTest {

    @Test
    void verifyFixedDelayRetryConfig() {
        var config = new Configuration();
        config.setString("http.source.lookup.retry-strategy.type", "fixed-delay");
        config.setString("http.source.lookup.retry-strategy.fixed-delay.delay", "10s");
        config.setInteger("lookup.max-retries", 12);

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
        config.setString("http.source.lookup.retry-strategy.type", "exponential-delay");

        config.setString(
                "http.source.lookup.retry-strategy.exponential-delay.initial-backoff", "15ms");
        config.setString(
                "http.source.lookup.retry-strategy.exponential-delay.max-backoff", "120ms");
        config.setInteger(
                "http.source.lookup.retry-strategy.exponential-delay.backoff-multiplier", 2);
        config.setInteger("lookup.max-retries", 6);

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
        config.setString("http.source.lookup.retry-strategy.type", "dummy");

        try (var mockedStatic = mockStatic(RetryStrategyType.class)) {
            var dummyStrategy = mock(RetryStrategyType.class);
            mockedStatic.when(() -> RetryStrategyType.fromCode("dummy")).thenReturn(dummyStrategy);

            assertThatThrownBy(() -> RetryConfigProvider.create(config))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }
}
