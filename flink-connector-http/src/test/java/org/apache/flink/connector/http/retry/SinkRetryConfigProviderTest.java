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

import org.apache.flink.connector.http.config.HttpSinkConfigFactory;
import org.apache.flink.connector.http.table.sink.Slf4jHttpPostRequestCallback;

import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.apache.flink.connector.http.config.HttpConnectorConfigConstants.SINK_MAX_RETRIES;
import static org.apache.flink.connector.http.config.HttpConnectorConfigConstants.SINK_RETRY_EXP_DELAY_INITIAL_BACKOFF;
import static org.apache.flink.connector.http.config.HttpConnectorConfigConstants.SINK_RETRY_EXP_DELAY_MAX_BACKOFF;
import static org.apache.flink.connector.http.config.HttpConnectorConfigConstants.SINK_RETRY_EXP_DELAY_MULTIPLIER;
import static org.apache.flink.connector.http.config.HttpConnectorConfigConstants.SINK_RETRY_FIXED_DELAY_DELAY;
import static org.apache.flink.connector.http.config.HttpConnectorConfigConstants.SINK_RETRY_STRATEGY_TYPE;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link SinkRetryConfigProvider}. */
class SinkRetryConfigProviderTest {

    @Test
    public void testFixedDelayRetryConfig() {
        Properties properties = new Properties();
        properties.setProperty(SINK_MAX_RETRIES, "2");
        properties.setProperty(SINK_RETRY_FIXED_DELAY_DELAY, "123ms");

        var retryConfig = SinkRetryConfigProvider.create(sinkConfig(properties));

        assertThat(retryConfig.getMaxAttempts()).isEqualTo(3);
        assertThat(retryConfig.getIntervalFunction().apply(1)).isEqualTo(123L);
        assertThat(retryConfig.getIntervalFunction().apply(2)).isEqualTo(123L);
    }

    @Test
    public void testExponentialDelayRetryConfig() {
        Properties properties = new Properties();
        properties.setProperty(SINK_MAX_RETRIES, "3");
        properties.setProperty(SINK_RETRY_STRATEGY_TYPE, "exponential-delay");
        properties.setProperty(SINK_RETRY_EXP_DELAY_INITIAL_BACKOFF, "100ms");
        properties.setProperty(SINK_RETRY_EXP_DELAY_MAX_BACKOFF, "1s");
        properties.setProperty(SINK_RETRY_EXP_DELAY_MULTIPLIER, "2");

        var retryConfig = SinkRetryConfigProvider.create(sinkConfig(properties));

        assertThat(retryConfig.getMaxAttempts()).isEqualTo(4);
        assertThat(retryConfig.getIntervalFunction().apply(1)).isEqualTo(100L);
        assertThat(retryConfig.getIntervalFunction().apply(2)).isEqualTo(200L);
        assertThat(retryConfig.getIntervalFunction().apply(5)).isEqualTo(1000L);
    }

    @Test
    public void testSkipsRetryStrategyWhenMaxRetriesIsZero() {
        Properties properties = new Properties();
        properties.setProperty(SINK_MAX_RETRIES, "0");
        properties.setProperty(SINK_RETRY_STRATEGY_TYPE, "not-a-strategy");

        var retryConfig = SinkRetryConfigProvider.create(sinkConfig(properties));

        assertThat(retryConfig.getMaxAttempts()).isEqualTo(1);
    }

    @Test
    public void testDefaultMaxRetriesUsesSingleAttempt() {
        var retryConfig = SinkRetryConfigProvider.create(sinkConfig(new Properties()));

        assertThat(retryConfig.getMaxAttempts()).isEqualTo(1);
    }

    private static org.apache.flink.connector.http.config.HttpSinkConfig sinkConfig(
            Properties properties) {
        return HttpSinkConfigFactory.fromDataStream(
                "http://localhost", properties, new Slf4jHttpPostRequestCallback());
    }
}
