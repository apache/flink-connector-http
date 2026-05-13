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

package org.apache.flink.connector.http.table.lookup;

import org.apache.flink.configuration.Configuration;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link HttpLookupConfig} serialization. */
public class HttpLookupConfigSerializationTest {

    @Test
    public void testHttpLookupConfigSerialization() throws Exception {
        // Create a Configuration with some values
        Configuration config = new Configuration();
        config.setString("test.key1", "value1");
        config.setString("test.key2", "value2");
        config.setString("test.number", "42");

        // Create HttpLookupConfig
        Properties props = new Properties();
        props.setProperty("prop1", "propValue1");

        HttpLookupConfig original =
                HttpLookupConfig.builder()
                        .lookupMethod("GET")
                        .url("http://localhost:8080")
                        .useAsync(false)
                        .properties(props)
                        .readableConfig(config)
                        .httpPostRequestCallback(new Slf4JHttpLookupPostRequestCallback())
                        .build();

        // Serialize
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(original);
        oos.close();

        // Deserialize
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        ObjectInputStream ois = new ObjectInputStream(bais);
        HttpLookupConfig deserialized = (HttpLookupConfig) ois.readObject();
        ois.close();

        // Verify basic fields
        assertThat(deserialized.getLookupMethod()).isEqualTo("GET");
        assertThat(deserialized.getUrl()).isEqualTo("http://localhost:8080");
        assertThat(deserialized.isUseAsync()).isFalse();
        assertThat(deserialized.getProperties().getProperty("prop1")).isEqualTo("propValue1");

        // Verify ReadableConfig is properly restored
        assertThat(deserialized.getReadableConfig()).isNotNull();
        assertThat(
                        deserialized
                                .getReadableConfig()
                                .get(
                                        org.apache.flink.configuration.ConfigOptions.key(
                                                        "test.key1")
                                                .stringType()
                                                .noDefaultValue()))
                .isEqualTo("value1");
        assertThat(
                        deserialized
                                .getReadableConfig()
                                .get(
                                        org.apache.flink.configuration.ConfigOptions.key(
                                                        "test.key2")
                                                .stringType()
                                                .noDefaultValue()))
                .isEqualTo("value2");
        assertThat(
                        deserialized
                                .getReadableConfig()
                                .get(
                                        org.apache.flink.configuration.ConfigOptions.key(
                                                        "test.number")
                                                .stringType()
                                                .noDefaultValue()))
                .isEqualTo("42");
    }

    @Test
    public void testHttpLookupConfigSerializationWithNullConfig() throws Exception {
        // Create HttpLookupConfig with null config
        HttpLookupConfig original =
                HttpLookupConfig.builder()
                        .lookupMethod("POST")
                        .url("http://localhost:9090")
                        .useAsync(true)
                        .properties(new Properties())
                        // Don't explicitly set readableConfig - let @Builder.Default create it
                        .httpPostRequestCallback(new Slf4JHttpLookupPostRequestCallback())
                        .build();

        // Serialize
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(original);
        oos.close();

        // Deserialize
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        ObjectInputStream ois = new ObjectInputStream(bais);
        HttpLookupConfig deserialized = (HttpLookupConfig) ois.readObject();
        ois.close();

        // Verify
        assertThat(deserialized.getLookupMethod()).isEqualTo("POST");
        assertThat(deserialized.getUrl()).isEqualTo("http://localhost:9090");
        assertThat(deserialized.isUseAsync()).isTrue();
        // ReadableConfig should be restored as empty Configuration, not null
        assertThat(deserialized.getReadableConfig()).isNotNull();
    }
}

// Made with Bob
