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
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.VarCharType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collection;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Unit tests for {@link HttpTableLookupFunction} populateNullJoinKeys method. */
@ExtendWith(MockitoExtension.class)
class HttpTableLookupFunctionPopulateJoinKeysTest {

    @Mock private PollingClientFactory pollingClientFactory;

    @Mock private PollingClient pollingClient;

    @Mock private DeserializationSchema<RowData> responseSchemaDecoder;

    private HttpLookupConfig httpLookupConfig;

    @BeforeEach
    void setUp() throws Exception {
        httpLookupConfig = HttpLookupConfig.builder().build();
        when(pollingClientFactory.createPollClient(any(), any())).thenReturn(pollingClient);

        // Mock pollingClient.open to do nothing
        Mockito.doNothing().when(pollingClient).open(any());
    }

    private FunctionContext createMockFunctionContext() {
        FunctionContext context = mock(FunctionContext.class);
        MetricGroup metricGroup = mock(MetricGroup.class);
        when(context.getMetricGroup()).thenReturn(metricGroup);
        when(metricGroup.gauge(anyString(), any())).thenReturn(null);
        return context;
    }

    @Test
    void testPopulateNullJoinKeys_singleJoinKey() throws Exception {
        // Setup: Create a schema with 3 fields: url (join key), name, value
        DataType producedDataType =
                DataTypes.ROW(
                        DataTypes.FIELD("url", DataTypes.STRING()),
                        DataTypes.FIELD("name", DataTypes.STRING()),
                        DataTypes.FIELD("value", DataTypes.STRING()));

        // Create lookup row with single join key "url"
        LookupRow lookupRow = new LookupRow();
        lookupRow.addLookupEntry(
                new RowDataSingleValueLookupSchemaEntry(
                        "url", RowData.createFieldGetter(new VarCharType(), 0)));
        lookupRow.setLookupPhysicalRowDataType(producedDataType);

        // Create the lookup function
        HttpTableLookupFunction lookupFunction =
                new HttpTableLookupFunction(
                        pollingClientFactory,
                        responseSchemaDecoder,
                        lookupRow,
                        httpLookupConfig,
                        new MetadataConverter[0],
                        null,
                        producedDataType);

        // Create keyRow with join key value
        GenericRowData keyRow = new GenericRowData(1);
        keyRow.setField(0, StringData.fromString("http://example.com"));

        // Create HTTP response row with null join key but populated other fields
        GenericRowData httpResponseRow = new GenericRowData(3);
        httpResponseRow.setField(0, null); // url is null
        httpResponseRow.setField(1, StringData.fromString("test-name"));
        httpResponseRow.setField(2, StringData.fromString("test-value"));

        // Mock the HTTP client to return the response
        HttpRowDataWrapper wrapper =
                HttpRowDataWrapper.builder()
                        .data(Collections.singletonList(httpResponseRow))
                        .httpCompletionState(HttpCompletionState.SUCCESS)
                        .httpStatusCode(200)
                        .build();
        when(pollingClient.pull(any())).thenReturn(wrapper);

        // Open the function
        FunctionContext context = createMockFunctionContext();
        lookupFunction.open(context);

        // Execute lookup
        Collection<RowData> result = lookupFunction.lookup(keyRow);

        // Verify: The result should have the join key populated
        assertThat(result).hasSize(1);
        RowData resultRow = result.iterator().next();
        assertThat(resultRow.getArity()).isEqualTo(3);
        assertThat(resultRow.getString(0)).isEqualTo(StringData.fromString("http://example.com"));
        assertThat(resultRow.getString(1)).isEqualTo(StringData.fromString("test-name"));
        assertThat(resultRow.getString(2)).isEqualTo(StringData.fromString("test-value"));
    }

    @Test
    void testPopulateNullJoinKeys_threeJoinKeys() throws Exception {
        // Setup: Create a schema with 5 fields: id, category, region (all join keys), name, value
        DataType producedDataType =
                DataTypes.ROW(
                        DataTypes.FIELD("id", DataTypes.STRING()),
                        DataTypes.FIELD("category", DataTypes.STRING()),
                        DataTypes.FIELD("region", DataTypes.STRING()),
                        DataTypes.FIELD("name", DataTypes.STRING()),
                        DataTypes.FIELD("value", DataTypes.STRING()));

        // Create lookup row with three join keys
        LookupRow lookupRow = new LookupRow();
        LogicalType stringType = new VarCharType();
        lookupRow.addLookupEntry(
                new RowDataSingleValueLookupSchemaEntry(
                        "id", RowData.createFieldGetter(stringType, 0)));
        lookupRow.addLookupEntry(
                new RowDataSingleValueLookupSchemaEntry(
                        "category", RowData.createFieldGetter(stringType, 1)));
        lookupRow.addLookupEntry(
                new RowDataSingleValueLookupSchemaEntry(
                        "region", RowData.createFieldGetter(stringType, 2)));
        lookupRow.setLookupPhysicalRowDataType(producedDataType);

        // Create the lookup function
        HttpTableLookupFunction lookupFunction =
                new HttpTableLookupFunction(
                        pollingClientFactory,
                        responseSchemaDecoder,
                        lookupRow,
                        httpLookupConfig,
                        new MetadataConverter[0],
                        null,
                        producedDataType);

        // Create keyRow with three join key values
        GenericRowData keyRow = new GenericRowData(3);
        keyRow.setField(0, StringData.fromString("id-123"));
        keyRow.setField(1, StringData.fromString("electronics"));
        keyRow.setField(2, StringData.fromString("us-west"));

        // Create HTTP response row with null join keys but populated other fields
        GenericRowData httpResponseRow = new GenericRowData(5);
        httpResponseRow.setField(0, null); // id is null
        httpResponseRow.setField(1, null); // category is null
        httpResponseRow.setField(2, null); // region is null
        httpResponseRow.setField(3, StringData.fromString("Product Name"));
        httpResponseRow.setField(4, StringData.fromString("Product Value"));

        // Mock the HTTP client to return the response
        HttpRowDataWrapper wrapper =
                HttpRowDataWrapper.builder()
                        .data(Collections.singletonList(httpResponseRow))
                        .httpCompletionState(HttpCompletionState.SUCCESS)
                        .httpStatusCode(200)
                        .build();
        when(pollingClient.pull(any())).thenReturn(wrapper);

        // Open the function
        FunctionContext context = createMockFunctionContext();
        lookupFunction.open(context);

        // Execute lookup
        Collection<RowData> result = lookupFunction.lookup(keyRow);

        // Verify: All three join keys should be populated
        assertThat(result).hasSize(1);
        RowData resultRow = result.iterator().next();
        assertThat(resultRow.getArity()).isEqualTo(5);
        assertThat(resultRow.getString(0)).isEqualTo(StringData.fromString("id-123"));
        assertThat(resultRow.getString(1)).isEqualTo(StringData.fromString("electronics"));
        assertThat(resultRow.getString(2)).isEqualTo(StringData.fromString("us-west"));
        assertThat(resultRow.getString(3)).isEqualTo(StringData.fromString("Product Name"));
        assertThat(resultRow.getString(4)).isEqualTo(StringData.fromString("Product Value"));
    }

    @Test
    void testPopulateNullJoinKeys_nullNonKeyFieldsRemainNull() throws Exception {
        // Setup: Create a schema with 5 fields: url (join key), name, value, description, status
        DataType producedDataType =
                DataTypes.ROW(
                        DataTypes.FIELD("url", DataTypes.STRING()),
                        DataTypes.FIELD("name", DataTypes.STRING()),
                        DataTypes.FIELD("value", DataTypes.STRING()),
                        DataTypes.FIELD("description", DataTypes.STRING()),
                        DataTypes.FIELD("status", DataTypes.STRING()));

        // Create lookup row with single join key "url"
        LookupRow lookupRow = new LookupRow();
        lookupRow.addLookupEntry(
                new RowDataSingleValueLookupSchemaEntry(
                        "url", RowData.createFieldGetter(new VarCharType(), 0)));
        lookupRow.setLookupPhysicalRowDataType(producedDataType);

        // Create the lookup function
        HttpTableLookupFunction lookupFunction =
                new HttpTableLookupFunction(
                        pollingClientFactory,
                        responseSchemaDecoder,
                        lookupRow,
                        httpLookupConfig,
                        new MetadataConverter[0],
                        null,
                        producedDataType);

        // Create keyRow with join key value
        GenericRowData keyRow = new GenericRowData(1);
        keyRow.setField(0, StringData.fromString("http://example.com"));

        // Create HTTP response row with null join key and some null non-key fields
        GenericRowData httpResponseRow = new GenericRowData(5);
        httpResponseRow.setField(0, null); // url is null (join key - will be populated)
        httpResponseRow.setField(1, StringData.fromString("test-name"));
        httpResponseRow.setField(2, null); // value is null (non-key - should remain null)
        httpResponseRow.setField(3, StringData.fromString("test-description"));
        httpResponseRow.setField(4, null); // status is null (non-key - should remain null)

        // Mock the HTTP client to return the response
        HttpRowDataWrapper wrapper =
                HttpRowDataWrapper.builder()
                        .data(Collections.singletonList(httpResponseRow))
                        .httpCompletionState(HttpCompletionState.SUCCESS)
                        .httpStatusCode(200)
                        .build();
        when(pollingClient.pull(any())).thenReturn(wrapper);

        // Open the function
        FunctionContext context = createMockFunctionContext();
        lookupFunction.open(context);

        // Execute lookup
        Collection<RowData> result = lookupFunction.lookup(keyRow);

        // Verify: Join key populated, non-key nulls remain null
        assertThat(result).hasSize(1);
        RowData resultRow = result.iterator().next();
        assertThat(resultRow.getArity()).isEqualTo(5);
        assertThat(resultRow.getString(0)).isEqualTo(StringData.fromString("http://example.com"));
        assertThat(resultRow.getString(1)).isEqualTo(StringData.fromString("test-name"));
        assertThat(resultRow.isNullAt(2)).isTrue(); // value should remain null
        assertThat(resultRow.getString(3)).isEqualTo(StringData.fromString("test-description"));
        assertThat(resultRow.isNullAt(4)).isTrue(); // status should remain null
    }

    @Test
    void testPopulateNullJoinKeys_nonNullFieldNotOverwritten() throws Exception {
        // Setup: Create a schema with 3 fields: url (join key), name, value
        DataType producedDataType =
                DataTypes.ROW(
                        DataTypes.FIELD("url", DataTypes.STRING()),
                        DataTypes.FIELD("name", DataTypes.STRING()),
                        DataTypes.FIELD("value", DataTypes.STRING()));

        // Create lookup row with single join key "url"
        LookupRow lookupRow = new LookupRow();
        lookupRow.addLookupEntry(
                new RowDataSingleValueLookupSchemaEntry(
                        "url", RowData.createFieldGetter(new VarCharType(), 0)));
        lookupRow.setLookupPhysicalRowDataType(producedDataType);

        // Create the lookup function
        HttpTableLookupFunction lookupFunction =
                new HttpTableLookupFunction(
                        pollingClientFactory,
                        responseSchemaDecoder,
                        lookupRow,
                        httpLookupConfig,
                        new MetadataConverter[0],
                        null,
                        producedDataType);

        // Create keyRow with join key value
        GenericRowData keyRow = new GenericRowData(1);
        keyRow.setField(0, StringData.fromString("http://example.com"));

        // Create HTTP response row with NON-NULL join key (should not be overwritten)
        GenericRowData httpResponseRow = new GenericRowData(3);
        httpResponseRow.setField(0, StringData.fromString("http://response-url.com"));
        httpResponseRow.setField(1, StringData.fromString("test-name"));
        httpResponseRow.setField(2, StringData.fromString("test-value"));

        // Mock the HTTP client to return the response
        HttpRowDataWrapper wrapper =
                HttpRowDataWrapper.builder()
                        .data(Collections.singletonList(httpResponseRow))
                        .httpCompletionState(HttpCompletionState.SUCCESS)
                        .httpStatusCode(200)
                        .build();
        when(pollingClient.pull(any())).thenReturn(wrapper);

        // Open the function
        FunctionContext context = createMockFunctionContext();
        lookupFunction.open(context);

        // Execute lookup
        Collection<RowData> result = lookupFunction.lookup(keyRow);

        // Verify: The non-null url from response should NOT be overwritten
        assertThat(result).hasSize(1);
        RowData resultRow = result.iterator().next();
        assertThat(resultRow.getString(0))
                .isEqualTo(StringData.fromString("http://response-url.com"));
        assertThat(resultRow.getString(1)).isEqualTo(StringData.fromString("test-name"));
        assertThat(resultRow.getString(2)).isEqualTo(StringData.fromString("test-value"));
    }
}
