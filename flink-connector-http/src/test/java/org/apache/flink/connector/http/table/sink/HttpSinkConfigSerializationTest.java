/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file distributed with
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

package org.apache.flink.connector.http.table.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.http.config.HttpSinkConfig;
import org.apache.flink.connector.http.config.HttpSinkConfigFactory;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.time.Duration;
import java.util.Properties;

import static org.apache.flink.connector.http.config.HttpConnectorConfigConstants.HTTP_ERROR_SINK_CODES_LIST;
import static org.apache.flink.connector.http.config.HttpConnectorConfigConstants.HTTP_ERROR_SINK_CODE_INCLUDE_LIST;
import static org.apache.flink.connector.http.table.sink.HttpDynamicSinkConnectorOptions.SINK_HTTP_IGNORED_RESPONSE_CODES;
import static org.apache.flink.connector.http.table.sink.HttpDynamicSinkConnectorOptions.SINK_HTTP_RETRY_CODES;
import static org.apache.flink.connector.http.table.sink.HttpDynamicSinkConnectorOptions.SINK_HTTP_SUCCESS_CODES;
import static org.apache.flink.connector.http.table.sink.HttpDynamicSinkConnectorOptions.SINK_MAX_RETRIES;
import static org.apache.flink.connector.http.table.sink.HttpDynamicSinkConnectorOptions.SINK_REQUEST_TIMEOUT;
import static org.apache.flink.connector.http.table.sink.HttpDynamicSinkConnectorOptions.SINK_RETRY_FIXED_DELAY_DELAY;
import static org.apache.flink.connector.http.table.sink.HttpDynamicSinkConnectorOptions.SINK_WRITER_THREAD_POOL_SIZE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link HttpSinkConfig} serialization. */
public class HttpSinkConfigSerializationTest {

    @Test
    public void testHttpSinkConfigSerialization() throws Exception {
        Configuration config = new Configuration();
        config.set(SINK_REQUEST_TIMEOUT, Duration.ofMinutes(2));
        config.set(SINK_WRITER_THREAD_POOL_SIZE, 3);
        config.set(SINK_MAX_RETRIES, 7);
        config.set(SINK_HTTP_SUCCESS_CODES, "2XX,404");
        config.set(SINK_HTTP_RETRY_CODES, "500");
        config.set(SINK_HTTP_IGNORED_RESPONSE_CODES, "404");
        config.set(SINK_RETRY_FIXED_DELAY_DELAY, Duration.ofSeconds(5));

        Properties props = new Properties();
        props.setProperty("prop1", "propValue1");

        HttpSinkConfig original =
                HttpSinkConfig.builder()
                        .url("http://localhost:8080")
                        .properties(props)
                        .readableConfig(config)
                        .httpPostRequestCallback(new Slf4jHttpPostRequestCallback())
                        .build();

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(original);
        oos.close();

        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        ObjectInputStream ois = new ObjectInputStream(bais);
        HttpSinkConfig deserialized = (HttpSinkConfig) ois.readObject();
        ois.close();

        assertThat(deserialized.getUrl()).isEqualTo("http://localhost:8080");
        assertThat(deserialized.getProperties().getProperty("prop1")).isEqualTo("propValue1");
        assertThat(deserialized.getRequestTimeout()).isEqualTo(Duration.ofMinutes(2));
        assertThat(deserialized.getWriterThreadPoolSize()).isEqualTo(3);
        assertThat(deserialized.getMaxRetries()).isEqualTo(7);
        assertThat(deserialized.getSuccessCodes()).isEqualTo("2XX,404");
        assertThat(deserialized.getRetryCodes()).isEqualTo("500");
        assertThat(deserialized.getIgnoredResponseCodes()).isEqualTo("404");
        assertThat(deserialized.getRetryFixedDelay()).isEqualTo(Duration.ofSeconds(5));
    }

    @Test
    public void testHttpSinkConfigDefaults() {
        HttpSinkConfig config =
                HttpSinkConfig.builder()
                        .url("http://localhost")
                        .httpPostRequestCallback(new Slf4jHttpPostRequestCallback())
                        .build();

        assertThat(config.getRequestTimeout()).isEqualTo(Duration.ofSeconds(30));
        assertThat(config.getWriterThreadPoolSize()).isEqualTo(4);
        assertThat(config.getMaxRetries()).isEqualTo(0);
        assertThat(config.getSuccessCodes()).isEqualTo("2XX");
        assertThat(config.getRetryCodes()).isEqualTo("500,503,504");
        assertThat(config.getIgnoredResponseCodes()).isEmpty();
    }

    @Test
    public void testFromDataStreamMergesPropertiesIntoReadableConfig() {
        Properties properties = new Properties();
        properties.setProperty(SINK_REQUEST_TIMEOUT.key(), "45s");
        properties.setProperty(SINK_WRITER_THREAD_POOL_SIZE.key(), "2");
        properties.setProperty(SINK_MAX_RETRIES.key(), "8");
        properties.setProperty(SINK_HTTP_SUCCESS_CODES.key(), "2XX,201");
        properties.setProperty(SINK_HTTP_RETRY_CODES.key(), "429,5XX");
        properties.setProperty(SINK_HTTP_IGNORED_RESPONSE_CODES.key(), "404");
        properties.setProperty(SINK_RETRY_FIXED_DELAY_DELAY.key(), "3s");

        HttpSinkConfig config =
                HttpSinkConfigFactory.fromDataStream(
                        "http://localhost", properties, new Slf4jHttpPostRequestCallback());

        assertThat(config.getRequestTimeout()).isEqualTo(Duration.ofSeconds(45));
        assertThat(config.getWriterThreadPoolSize()).isEqualTo(2);
        assertThat(config.getMaxRetries()).isEqualTo(8);
        assertThat(config.getSuccessCodes()).isEqualTo("2XX,201");
        assertThat(config.getRetryCodes()).isEqualTo("429,5XX");
        assertThat(config.getIgnoredResponseCodes()).isEqualTo("404");
        assertThat(config.getRetryFixedDelay()).isEqualTo(Duration.ofSeconds(3));
    }

    @Test
    public void testRejectsLegacyExcludeTogetherWithIgnoredResponseCodes() {
        Properties properties = new Properties();
        properties.setProperty(HTTP_ERROR_SINK_CODE_INCLUDE_LIST, "404");
        properties.setProperty(SINK_HTTP_IGNORED_RESPONSE_CODES.key(), "409");

        assertThatThrownBy(
                        () ->
                                HttpSinkConfigFactory.fromDataStream(
                                        "http://localhost",
                                        properties,
                                        new Slf4jHttpPostRequestCallback()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(HTTP_ERROR_SINK_CODE_INCLUDE_LIST)
                .hasMessageContaining(SINK_HTTP_IGNORED_RESPONSE_CODES.key());
    }

    @Test
    public void testLegacyIncludeListMapsToIgnoredResponseCodes() {
        Properties properties = new Properties();
        properties.setProperty(HTTP_ERROR_SINK_CODE_INCLUDE_LIST, "404,405");

        HttpSinkConfig config =
                HttpSinkConfigFactory.fromDataStream(
                        "http://localhost", properties, new Slf4jHttpPostRequestCallback());

        assertThat(config.getIgnoredResponseCodes()).isEqualTo("404,405");
    }

    @Test
    public void testTableOptionsMapLegacyIncludeListToIgnoredResponseCodes() {
        Configuration configuration = new Configuration();
        configuration.set(HttpDynamicSinkConnectorOptions.URL, "http://localhost");

        Properties properties = new Properties();
        properties.setProperty(HTTP_ERROR_SINK_CODE_INCLUDE_LIST, "404,405");

        HttpSinkConfig config =
                HttpSinkConfigFactory.fromTableOptions(
                        configuration, properties, new Slf4jHttpPostRequestCallback());

        assertThat(config.getIgnoredResponseCodes()).isEqualTo("404,405");
    }

    @Test
    public void testFromDataStreamRejectsLegacyErrorCodeWithSuccessCodes() {
        Properties properties = new Properties();
        properties.setProperty(HTTP_ERROR_SINK_CODES_LIST, "4XX");
        properties.setProperty(SINK_HTTP_SUCCESS_CODES.key(), "2XX");

        assertThatThrownBy(
                        () ->
                                HttpSinkConfigFactory.fromDataStream(
                                        "http://localhost",
                                        properties,
                                        new Slf4jHttpPostRequestCallback()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Cannot set legacy HTTP sink error-code properties")
                .hasMessageContaining(SINK_HTTP_SUCCESS_CODES.key())
                .hasMessageNotContaining(SINK_HTTP_RETRY_CODES.key());
    }

    @Test
    public void testFromTableOptionsRejectsLegacyErrorCodeWithRetryCodes() {
        Configuration configuration = new Configuration();
        configuration.set(HttpDynamicSinkConnectorOptions.URL, "http://localhost");
        configuration.set(SINK_HTTP_RETRY_CODES, "500,503");

        Properties properties = new Properties();
        properties.setProperty(HTTP_ERROR_SINK_CODES_LIST, "4XX");

        assertThatThrownBy(
                        () ->
                                HttpSinkConfigFactory.fromTableOptions(
                                        configuration,
                                        properties,
                                        new Slf4jHttpPostRequestCallback()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Cannot set legacy HTTP sink error-code properties")
                .hasMessageContaining(SINK_HTTP_RETRY_CODES.key())
                .hasMessageNotContaining(SINK_HTTP_SUCCESS_CODES.key());
    }

    @Test
    public void testFromDataStreamRejectsLegacyExcludeWithRetryCodes() {
        Properties properties = new Properties();
        properties.setProperty(HTTP_ERROR_SINK_CODE_INCLUDE_LIST, "404");
        properties.setProperty(SINK_HTTP_RETRY_CODES.key(), "500,503");

        assertThatThrownBy(
                        () ->
                                HttpSinkConfigFactory.fromDataStream(
                                        "http://localhost",
                                        properties,
                                        new Slf4jHttpPostRequestCallback()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Cannot set legacy HTTP sink error-code properties")
                .hasMessageContaining(HTTP_ERROR_SINK_CODE_INCLUDE_LIST)
                .hasMessageContaining(SINK_HTTP_RETRY_CODES.key())
                .hasMessageNotContaining(SINK_HTTP_SUCCESS_CODES.key());
    }

    @Test
    public void testFromDataStreamAllowsLegacyErrorCodeWithIgnoredResponseCodes() {
        Properties properties = new Properties();
        properties.setProperty(HTTP_ERROR_SINK_CODES_LIST, "4XX");
        properties.setProperty(SINK_HTTP_IGNORED_RESPONSE_CODES.key(), "404");

        HttpSinkConfig config =
                HttpSinkConfigFactory.fromDataStream(
                        "http://localhost", properties, new Slf4jHttpPostRequestCallback());

        assertThat(config.getIgnoredResponseCodes()).isEqualTo("404");
        assertThat(config.getProperties().getProperty(HTTP_ERROR_SINK_CODES_LIST)).isEqualTo("4XX");
    }
}
