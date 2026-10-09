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

package org.apache.flink.connector.http.table.lookup;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.connector.http.clients.PollingClient;
import org.apache.flink.connector.http.clients.PollingClientFactory;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.VarCharType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.type;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

/** Tests the rows {@link HttpTableLookupFunction} emits for responses without data. */
@ExtendWith(MockitoExtension.class)
class HttpTableLookupFunctionTest {

    private static final DataType PHYSICAL_ROW_DATA_TYPE =
            DataTypes.ROW(
                    DataTypes.FIELD("id", DataTypes.STRING()),
                    DataTypes.FIELD("name", DataTypes.STRING()),
                    DataTypes.FIELD("age", DataTypes.INT()));

    private static final StringData KEY = StringData.fromString("1");

    @Mock private PollingClientFactory pollingClientFactory;

    @Mock private PollingClient pollingClient;

    @Mock private DeserializationSchema<RowData> responseSchemaDecoder;

    @Mock private FunctionContext context;

    @BeforeEach
    void setUp() throws Exception {
        when(pollingClientFactory.createPollClient(any(), any())).thenReturn(pollingClient);
        when(context.getMetricGroup()).thenReturn(new UnregisteredMetricsGroup());
    }

    /**
     * Without metadata columns, a response without data is a lookup miss, failed calls included.
     */
    @ParameterizedTest
    @EnumSource
    void noDataWithoutMetadataEmitsNoRow(final HttpCompletionState completionState)
            throws Exception {
        final HttpTableLookupFunction lookupFunction = openLookupFunction();
        when(pollingClient.pull(any())).thenReturn(noDataResponse(completionState));

        assertThat(lookupFunction.lookup(GenericRowData.of(KEY))).isEmpty();
    }

    @ParameterizedTest
    @EnumSource
    void noDataWithMetadataEmitsMetadataRow(final HttpCompletionState completionState)
            throws Exception {
        final HttpTableLookupFunction lookupFunction =
                openLookupFunction(new MetadataConverter.HttpCompletionStateConverter());
        when(pollingClient.pull(any())).thenReturn(noDataResponse(completionState));

        assertThat(lookupFunction.lookup(GenericRowData.of(KEY)))
                .singleElement(type(GenericRowData.class))
                .returns(4, GenericRowData::getArity)
                .returns(null, row -> row.getField(0))
                .returns(null, row -> row.getField(1))
                .returns(null, row -> row.getField(2))
                .returns(StringData.fromString(completionState.name()), row -> row.getField(3));
    }

    private HttpTableLookupFunction openLookupFunction(
            final MetadataConverter... metadataConverters) throws Exception {
        final LookupRow lookupRow = new LookupRow();
        lookupRow.addLookupEntry(
                new RowDataSingleValueLookupSchemaEntry(
                        "id", RowData.createFieldGetter(new VarCharType(), 0)));
        final HttpTableLookupFunction lookupFunction =
                new HttpTableLookupFunction(
                        pollingClientFactory,
                        responseSchemaDecoder,
                        lookupRow,
                        HttpLookupConfig.builder().build(),
                        metadataConverters,
                        PHYSICAL_ROW_DATA_TYPE);
        lookupFunction.open(context);
        return lookupFunction;
    }

    private static HttpRowDataWrapper noDataResponse(final HttpCompletionState completionState) {
        return HttpRowDataWrapper.builder()
                .data(Collections.emptyList())
                .httpCompletionState(completionState)
                .build();
    }
}
