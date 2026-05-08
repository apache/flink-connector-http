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

package org.apache.flink.connector.http.sink;

import org.apache.flink.connector.http.config.HttpConnectorConfigConstants;

import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for {@link SinkRetryConfig}. */
class SinkRetryConfigTest {

    @Test
    void shouldUseDefaultsWhenNoPropertiesSet() {
        var cfg = SinkRetryConfig.fromProperties(new Properties());

        assertThat(cfg.getMaxRetries()).isEqualTo(SinkRetryConfig.DEFAULT_MAX_RETRIES);
        // Default success checker treats 2xx as success and {500,503,504} as retryable.
        assertThat(cfg.isSuccess(200)).isTrue();
        assertThat(cfg.isSuccess(201)).isTrue();
        assertThat(cfg.shouldRetry(500)).isTrue();
        assertThat(cfg.shouldRetry(503)).isTrue();
        assertThat(cfg.shouldRetry(504)).isTrue();
        // 404 is neither success nor retryable — fatal.
        assertThat(cfg.isSuccess(404)).isFalse();
        assertThat(cfg.shouldRetry(404)).isFalse();
    }

    @Test
    void shouldHonourFixedDelayStrategy() {
        Properties props = new Properties();
        props.setProperty(HttpConnectorConfigConstants.SINK_HTTP_RETRY_TIMES, "5");
        props.setProperty(HttpConnectorConfigConstants.SINK_RETRY_STRATEGY_TYPE, "fixed-delay");
        props.setProperty(HttpConnectorConfigConstants.SINK_RETRY_FIXED_DELAY_DELAY, "250ms");

        var cfg = SinkRetryConfig.fromProperties(props);

        assertThat(cfg.getMaxRetries()).isEqualTo(5);
        // Fixed delay — every attempt returns the same value.
        assertThat(cfg.backoffMillis(0)).isEqualTo(250L);
        assertThat(cfg.backoffMillis(3)).isEqualTo(250L);
    }

    @Test
    void shouldHonourExponentialDelayStrategy() {
        Properties props = new Properties();
        props.setProperty(
                HttpConnectorConfigConstants.SINK_RETRY_STRATEGY_TYPE, "exponential-delay");
        props.setProperty(HttpConnectorConfigConstants.SINK_RETRY_EXP_DELAY_INITIAL_BACKOFF, "1s");
        props.setProperty(HttpConnectorConfigConstants.SINK_RETRY_EXP_DELAY_MAX_BACKOFF, "10s");
        props.setProperty(HttpConnectorConfigConstants.SINK_RETRY_EXP_DELAY_MULTIPLIER, "2.0");

        var cfg = SinkRetryConfig.fromProperties(props);

        assertThat(cfg.backoffMillis(0)).isEqualTo(1000L);
        assertThat(cfg.backoffMillis(1)).isEqualTo(2000L);
        assertThat(cfg.backoffMillis(2)).isEqualTo(4000L);
        // Eventually capped at the configured max (10s).
        assertThat(cfg.backoffMillis(10)).isEqualTo(10_000L);
    }

    @Test
    void shouldAcceptIsoDurationsAlongsideFlinkFormat() {
        // HttpDynamicSink writes ConfigOption<Duration> values as Duration#toString (ISO-8601),
        // so the parser must accept both formats.
        Properties props = new Properties();
        props.setProperty(
                HttpConnectorConfigConstants.SINK_RETRY_EXP_DELAY_INITIAL_BACKOFF, "PT2S");
        props.setProperty(HttpConnectorConfigConstants.SINK_RETRY_EXP_DELAY_MULTIPLIER, "3.0");

        var cfg = SinkRetryConfig.fromProperties(props);

        assertThat(cfg.backoffMillis(0)).isEqualTo(2000L);
    }

    @Test
    void shouldHonourCustomSuccessAndRetryCodes() {
        Properties props = new Properties();
        props.setProperty(HttpConnectorConfigConstants.SINK_RETRY_SUCCESS_CODES, "2XX,201");
        props.setProperty(HttpConnectorConfigConstants.SINK_RETRY_RETRY_CODES, "429,502,503");

        var cfg = SinkRetryConfig.fromProperties(props);

        assertThat(cfg.shouldRetry(429)).isTrue();
        assertThat(cfg.shouldRetry(502)).isTrue();
        assertThat(cfg.shouldRetry(500)).isFalse();
        assertThat(cfg.isSuccess(200)).isTrue();
    }

    @Test
    void shouldRejectInvalidStrategy() {
        Properties props = new Properties();
        props.setProperty(HttpConnectorConfigConstants.SINK_RETRY_STRATEGY_TYPE, "not-a-strategy");

        assertThatThrownBy(() -> SinkRetryConfig.fromProperties(props))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldRejectInvalidDuration() {
        Properties props = new Properties();
        props.setProperty(HttpConnectorConfigConstants.SINK_RETRY_FIXED_DELAY_DELAY, "???");
        props.setProperty(HttpConnectorConfigConstants.SINK_RETRY_STRATEGY_TYPE, "fixed-delay");

        assertThatThrownBy(() -> SinkRetryConfig.fromProperties(props))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(HttpConnectorConfigConstants.SINK_RETRY_FIXED_DELAY_DELAY);
    }
}
