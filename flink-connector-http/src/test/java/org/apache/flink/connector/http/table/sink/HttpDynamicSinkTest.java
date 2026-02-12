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

package org.apache.flink.connector.http.table.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.http.table.sink.HttpDynamicSink.HttpDynamicTableSinkBuilder;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.TestFormatFactory;
import org.apache.flink.table.types.AtomicDataType;
import org.apache.flink.table.types.logical.BooleanType;

import org.junit.jupiter.api.Test;

import static org.apache.flink.connector.http.table.sink.HttpDynamicSinkConnectorOptions.INSERT_METHOD;
import static org.apache.flink.connector.http.table.sink.HttpDynamicSinkConnectorOptions.URL;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link HttpDynamicSink}. */
public class HttpDynamicSinkTest {

    @Test
    public void testAsSummaryString() {
        var mockFormat = new TestFormatFactory.EncodingFormatMock(",", ChangelogMode.insertOnly());

        HttpDynamicSink dynamicSink =
                new HttpDynamicTableSinkBuilder()
                        .setTableOptions(new Configuration())
                        .setConsumedDataType(new AtomicDataType(new BooleanType(false)))
                        .setEncodingFormat(mockFormat)
                        .setHttpPostRequestCallback(new Slf4jHttpPostRequestCallback())
                        .build();

        assertThat(dynamicSink.asSummaryString()).isEqualTo("HttpSink");
    }

    @Test
    public void copyEqualityTest() {
        var mockFormat = new TestFormatFactory.EncodingFormatMock(",", ChangelogMode.insertOnly());
        var sink =
                new HttpDynamicTableSinkBuilder()
                        .setTableOptions(
                                new Configuration() {
                                    {
                                        this.set(URL, "localhost:8123");
                                        this.set(INSERT_METHOD, "POST");
                                        this.set(FactoryUtil.FORMAT, "json");
                                    }
                                })
                        .setConsumedDataType(new AtomicDataType(new BooleanType(false)))
                        .setEncodingFormat(mockFormat)
                        .setHttpPostRequestCallback(new Slf4jHttpPostRequestCallback())
                        .build();

        assertThat(sink.copy()).isEqualTo(sink);
        assertThat(sink.copy().hashCode()).isEqualTo(sink.hashCode());
    }

    private HttpDynamicTableSinkBuilder getSinkBuilder() {
        var mockFormat = new TestFormatFactory.EncodingFormatMock(",", ChangelogMode.insertOnly());
        var consumedDataType = new AtomicDataType(new BooleanType(false));

        return new HttpDynamicTableSinkBuilder()
                .setTableOptions(
                        new Configuration() {
                            {
                                this.set(URL, "localhost:8123");
                                this.set(INSERT_METHOD, "POST");
                                this.set(FactoryUtil.FORMAT, "json");
                            }
                        })
                .setConsumedDataType(consumedDataType)
                .setEncodingFormat(mockFormat)
                .setHttpPostRequestCallback(new Slf4jHttpPostRequestCallback())
                .setMaxBatchSize(1);
    }

    @Test
    public void nonEqualsTest() {
        var sink = getSinkBuilder().build();
        var sinkBatchSize = getSinkBuilder().setMaxBatchSize(10).build();
        var sinkSinkConfig =
                getSinkBuilder()
                        .setTableOptions(
                                new Configuration() {
                                    {
                                        this.set(URL, "localhost:8124");
                                        this.set(INSERT_METHOD, "POST");
                                        this.set(FactoryUtil.FORMAT, "json");
                                    }
                                })
                        .build();
        var sinkDataType =
                getSinkBuilder()
                        .setConsumedDataType(new AtomicDataType(new BooleanType(true)))
                        .build();
        var sinkFormat =
                getSinkBuilder()
                        .setEncodingFormat(
                                new TestFormatFactory.EncodingFormatMock(";", ChangelogMode.all()))
                        .build();
        var sinkHttpPostRequestCallback =
                getSinkBuilder()
                        .setHttpPostRequestCallback(new Slf4jHttpPostRequestCallback())
                        .build();

        assertThat(sink).isEqualTo(sink);
        assertThat(sink).isNotEqualTo(null);
        assertThat(sink).isNotEqualTo("test-string");
        assertThat(sink).isNotEqualTo(sinkBatchSize);
        assertThat(sink).isNotEqualTo(sinkSinkConfig);
        assertThat(sink).isNotEqualTo(sinkDataType);
        assertThat(sink).isNotEqualTo(sinkFormat);
        assertThat(sink).isNotEqualTo(sinkHttpPostRequestCallback);
    }
}
