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

package org.apache.flink.connector.http.table.lookup.querycreators;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.http.table.lookup.LookupQueryInfo;
import org.apache.flink.connector.http.table.lookup.LookupRow;
import org.apache.flink.connector.http.table.lookup.RowDataSingleValueLookupSchemaEntry;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.FieldsDataType;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.Map;

import static org.apache.flink.connector.http.table.lookup.HttpLookupConnectorOptions.LOOKUP_METHOD;
import static org.apache.flink.connector.http.table.lookup.HttpLookupTableSourceFactory.row;
import static org.apache.flink.connector.http.table.lookup.querycreators.QueryCreatorUtils.getTableContext;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Test for {@link GenericJsonQueryCreator}. */
class GenericJsonAndUrlQueryCreatorTest {
    private static final String KEY_1 = "key1";
    private static final String KEY_2 = "key2";
    private static final String KEY_3 = "key3";
    private static final String VALUE = "val1";
    // for GET this is the minimum config
    private static final List<String> QUERY_PARAMS = List.of(KEY_1);
    // Path param ArgPath required a stringified json object. As we have PersonBean
    // we can use that.
    private static final Map<String, String> urlParams = Map.of(KEY_1, KEY_1);
    private static final DataType DATATYPE_1 =
            row(List.of(DataTypes.FIELD(KEY_1, DataTypes.STRING())));
    private static final DataType DATATYPE_1_2 =
            row(
                    List.of(
                            DataTypes.FIELD(KEY_1, DataTypes.STRING()),
                            DataTypes.FIELD(KEY_2, DataTypes.STRING())));
    private static final ResolvedSchema RESOLVED_SCHEMA =
            ResolvedSchema.of(Column.physical(KEY_1, DataTypes.STRING()));
    private static final RowData ROWDATA = getRowData(1, VALUE);

    @ParameterizedTest
    @ValueSource(strings = {"GET", "PUT", "POST"})
    public void createLookupQueryTestStrAllOps(String operation) {
        // GIVEN
        LookupRow lookupRow = getLookupRow(KEY_1);
        Configuration config = getConfiguration(operation);
        GenericJsonAndUrlQueryCreator universalJsonQueryCreator =
                (GenericJsonAndUrlQueryCreator)
                        new GenericJsonAndUrlQueryCreatorFactory()
                                .createLookupQueryCreator(
                                        config,
                                        lookupRow,
                                        getTableContext(config, RESOLVED_SCHEMA));
        // WHEN
        var createdQuery = universalJsonQueryCreator.createLookupQuery(ROWDATA);
        // THEN
        if (operation.equals("GET")) {
            validateCreatedQueryForGet(createdQuery);
        } else {
            validateCreatedQueryForPutAndPost(createdQuery);
        }
        // validate url based parameters
        assertThat(createdQuery.getPathBasedUrlParameters().size() == 1).isTrue();
        assertThat(createdQuery.getPathBasedUrlParameters().get(KEY_1)).isEqualTo(VALUE);
    }

    @Test
    public void createLookupQueryTest() {
        // GIVEN
        List<String> queryParams = List.of(KEY_1, KEY_2);
        final String urlInsert = "AAA";
        Map<String, String> urlParams = Map.of(KEY_1, urlInsert);
        LookupRow lookupRow = getLookupRow(KEY_1, KEY_2);
        ResolvedSchema resolvedSchema =
                ResolvedSchema.of(
                        Column.physical(KEY_1, DataTypes.STRING()),
                        Column.physical(KEY_2, DataTypes.STRING()));
        Configuration config = getConfiguration("GET");
        config.set(GenericJsonAndUrlQueryCreatorFactory.REQUEST_QUERY_PARAM_FIELDS, queryParams);
        config.set(GenericJsonAndUrlQueryCreatorFactory.REQUEST_URL_MAP, urlParams);
        lookupRow.setLookupPhysicalRowDataType(DATATYPE_1_2);
        GenericJsonAndUrlQueryCreator genericJsonAndUrlQueryCreator =
                (GenericJsonAndUrlQueryCreator)
                        new GenericJsonAndUrlQueryCreatorFactory()
                                .createLookupQueryCreator(
                                        config, lookupRow, getTableContext(config, resolvedSchema));
        var row = getRowData(2, VALUE);
        row.setField(1, StringData.fromString(VALUE));
        // WHEN
        var createdQuery = genericJsonAndUrlQueryCreator.createLookupQuery(row);
        // THEN
        assertThat(createdQuery.getPathBasedUrlParameters().get(urlInsert)).isEqualTo(VALUE);
        assertThat(createdQuery.getBodyBasedUrlQueryParameters()).isEmpty();
        assertThat(createdQuery.getLookupQuery())
                .isEqualTo(KEY_1 + "=" + VALUE + "&" + KEY_2 + "=" + VALUE);
    }

    @Test
    public void failSerializationOpenTest() {
        // GIVEN
        LookupRow lookupRow = getLookupRow(KEY_1);
        ResolvedSchema resolvedSchema =
                ResolvedSchema.of(Column.physical(KEY_1, DataTypes.STRING()));
        Configuration config = getConfiguration("GET");
        lookupRow.setLookupPhysicalRowDataType(DATATYPE_1);
        GenericJsonAndUrlQueryCreator genericJsonAndUrlQueryCreator =
                (GenericJsonAndUrlQueryCreator)
                        new GenericJsonAndUrlQueryCreatorFactory()
                                .createLookupQueryCreator(
                                        config, lookupRow, getTableContext(config, resolvedSchema));
        // create a SerializationSchema that throws and exception in open
        SerializationSchema<RowData> mockSerialiationSchema =
                new SerializationSchema<RowData>() {
                    @Override
                    public void open(InitializationContext context) throws Exception {
                        throw new Exception("Exception for testing");
                    }

                    @Override
                    public byte[] serialize(RowData element) {
                        return new byte[0];
                    }
                };
        // WHEN
        genericJsonAndUrlQueryCreator.setSerializationSchema(mockSerialiationSchema);
        var row = new GenericRowData(1);
        // THEN
        assertThrows(
                RuntimeException.class,
                () -> {
                    genericJsonAndUrlQueryCreator.createLookupQuery(row);
                });
    }

    @Test
    void convertToQueryParametersUnsupportedEncodingTest() {
        // GIVEN
        ObjectMapper mapper = ObjectMapperAdapter.instance();
        PersonBean person = new PersonBean("aaa", "bbb");
        // WHEN
        JsonNode personNode = mapper.valueToTree(person);
        // THEN
        assertThrows(
                RuntimeException.class,
                () -> {
                    GenericJsonAndUrlQueryCreator.convertToQueryParameters(
                            (ObjectNode) personNode, "bad encoding");
                });
    }

    @Test
    void rowDataToRowTest() {
        // GIVEN
        // String
        final String value = VALUE;
        int intValue = 10;
        GenericRowData rowData =
                GenericRowData.of(StringData.fromString(value), intValue, intValue);
        DataType dataType =
                row(
                        List.of(
                                DataTypes.FIELD(KEY_1, DataTypes.STRING()),
                                DataTypes.FIELD(KEY_2, DataTypes.DATE()),
                                DataTypes.FIELD(KEY_3, DataTypes.TIMESTAMP_LTZ())));
        // WHEN
        Row row = rowDataToRow(rowData, dataType);
        // THEN
        assertThat(row.getField(KEY_1).equals(value));
        assertThat(row.getField(KEY_2).equals("1970-01-01T00:00:00.010"));
        assertThat(row.getField(KEY_3).equals("1970-01-01T00:00:00.010Z"));
    }

    @Test
    public void testAdditionalJsonSimpleFields() throws Exception {
        // GIVEN - Simple additional fields
        LookupRow lookupRow = getLookupRow(KEY_1);
        Configuration config = getConfiguration("POST");
        config.set(
                GenericJsonAndUrlQueryCreatorFactory.REQUEST_ADDITIONAL_BODY_JSON,
                "{\"c\":789,\"d\":\"extra\"}");

        GenericJsonAndUrlQueryCreator creator =
                (GenericJsonAndUrlQueryCreator)
                        new GenericJsonAndUrlQueryCreatorFactory()
                                .createLookupQueryCreator(
                                        config,
                                        lookupRow,
                                        getTableContext(config, RESOLVED_SCHEMA));

        // WHEN
        var createdQuery = creator.createLookupQuery(ROWDATA);

        // THEN
        String expectedJson = "{\"key1\":\"val1\",\"c\":789,\"d\":\"extra\"}";
        ObjectMapper mapper = new ObjectMapper();
        JsonNode expected = mapper.readTree(expectedJson);
        JsonNode actual = mapper.readTree(createdQuery.getLookupQuery());
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void testAdditionalJsonNestedObject() throws Exception {
        // GIVEN - Nested object in additional JSON
        LookupRow lookupRow = getLookupRow(KEY_1);
        Configuration config = getConfiguration("POST");
        config.set(
                GenericJsonAndUrlQueryCreatorFactory.REQUEST_ADDITIONAL_BODY_JSON,
                "{\"nested\":{\"field1\":\"value1\",\"field2\":123}}");

        GenericJsonAndUrlQueryCreator creator =
                (GenericJsonAndUrlQueryCreator)
                        new GenericJsonAndUrlQueryCreatorFactory()
                                .createLookupQueryCreator(
                                        config,
                                        lookupRow,
                                        getTableContext(config, RESOLVED_SCHEMA));

        // WHEN
        var createdQuery = creator.createLookupQuery(ROWDATA);

        // THEN
        String expectedJson =
                "{\"key1\":\"val1\",\"nested\":{\"field1\":\"value1\",\"field2\":123}}";
        ObjectMapper mapper = new ObjectMapper();
        JsonNode expected = mapper.readTree(expectedJson);
        JsonNode actual = mapper.readTree(createdQuery.getLookupQuery());
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void testAdditionalJsonMultipleNestedObjects() throws Exception {
        // GIVEN - Multiple nested objects
        LookupRow lookupRow = getLookupRow(KEY_1);
        Configuration config = getConfiguration("POST");
        config.set(
                GenericJsonAndUrlQueryCreatorFactory.REQUEST_ADDITIONAL_BODY_JSON,
                "{\"obj1\":{\"a\":1,\"b\":2},\"obj2\":{\"c\":3,\"d\":4}}");

        GenericJsonAndUrlQueryCreator creator =
                (GenericJsonAndUrlQueryCreator)
                        new GenericJsonAndUrlQueryCreatorFactory()
                                .createLookupQueryCreator(
                                        config,
                                        lookupRow,
                                        getTableContext(config, RESOLVED_SCHEMA));

        // WHEN
        var createdQuery = creator.createLookupQuery(ROWDATA);

        // THEN
        String expectedJson =
                "{\"key1\":\"val1\",\"obj1\":{\"a\":1,\"b\":2},\"obj2\":{\"c\":3,\"d\":4}}";
        ObjectMapper mapper = new ObjectMapper();
        JsonNode expected = mapper.readTree(expectedJson);
        JsonNode actual = mapper.readTree(createdQuery.getLookupQuery());
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void testAdditionalJsonWithArray() throws Exception {
        // GIVEN - Array in additional JSON
        LookupRow lookupRow = getLookupRow(KEY_1);
        Configuration config = getConfiguration("POST");
        config.set(
                GenericJsonAndUrlQueryCreatorFactory.REQUEST_ADDITIONAL_BODY_JSON,
                "{\"items\":[\"item1\",\"item2\",\"item3\"]}");

        GenericJsonAndUrlQueryCreator creator =
                (GenericJsonAndUrlQueryCreator)
                        new GenericJsonAndUrlQueryCreatorFactory()
                                .createLookupQueryCreator(
                                        config,
                                        lookupRow,
                                        getTableContext(config, RESOLVED_SCHEMA));

        // WHEN
        var createdQuery = creator.createLookupQuery(ROWDATA);

        // THEN
        String expectedJson = "{\"key1\":\"val1\",\"items\":[\"item1\",\"item2\",\"item3\"]}";
        ObjectMapper mapper = new ObjectMapper();
        JsonNode expected = mapper.readTree(expectedJson);
        JsonNode actual = mapper.readTree(createdQuery.getLookupQuery());
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void testAdditionalJsonComplexStructure() throws Exception {
        // GIVEN - Complex nested structure with arrays and objects
        LookupRow lookupRow = getLookupRow(KEY_1);
        Configuration config = getConfiguration("POST");
        config.set(
                GenericJsonAndUrlQueryCreatorFactory.REQUEST_ADDITIONAL_BODY_JSON,
                "{\"metadata\":{\"tags\":[\"tag1\",\"tag2\"],\"count\":5},\"flags\":[true,false]}");
        GenericJsonAndUrlQueryCreator creator =
                (GenericJsonAndUrlQueryCreator)
                        new GenericJsonAndUrlQueryCreatorFactory()
                                .createLookupQueryCreator(
                                        config,
                                        lookupRow,
                                        getTableContext(config, RESOLVED_SCHEMA));
        // WHEN
        var createdQuery = creator.createLookupQuery(ROWDATA);

        // THEN
        String expectedJson =
                "{\"key1\":\"val1\",\"metadata\":{\"tags\":[\"tag1\",\"tag2\"],\"count\":5},\"flags\":[true,false]}";
        ObjectMapper mapper = new ObjectMapper();
        JsonNode expected = mapper.readTree(expectedJson);
        JsonNode actual = mapper.readTree(createdQuery.getLookupQuery());
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void testAdditionalJsonWithBoolean() throws Exception {
        // GIVEN - Boolean values in additional JSON
        LookupRow lookupRow = getLookupRow(KEY_1);
        Configuration config = getConfiguration("POST");
        config.set(
                GenericJsonAndUrlQueryCreatorFactory.REQUEST_ADDITIONAL_BODY_JSON,
                "{\"isActive\":true,\"isDeleted\":false}");

        GenericJsonAndUrlQueryCreator creator =
                (GenericJsonAndUrlQueryCreator)
                        new GenericJsonAndUrlQueryCreatorFactory()
                                .createLookupQueryCreator(
                                        config,
                                        lookupRow,
                                        getTableContext(config, RESOLVED_SCHEMA));

        // WHEN
        var createdQuery = creator.createLookupQuery(ROWDATA);

        // THEN
        String expectedJson = "{\"key1\":\"val1\",\"isActive\":true,\"isDeleted\":false}";
        ObjectMapper mapper = new ObjectMapper();
        JsonNode expected = mapper.readTree(expectedJson);
        JsonNode actual = mapper.readTree(createdQuery.getLookupQuery());
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void testAdditionalJsonNullOrEmpty() {
        // GIVEN - Null additional JSON
        LookupRow lookupRow = getLookupRow(KEY_1);
        Configuration config = getConfiguration("POST");
        // No additional JSON set

        GenericJsonAndUrlQueryCreator creator =
                (GenericJsonAndUrlQueryCreator)
                        new GenericJsonAndUrlQueryCreatorFactory()
                                .createLookupQueryCreator(
                                        config,
                                        lookupRow,
                                        getTableContext(config, RESOLVED_SCHEMA));

        // WHEN
        var createdQuery = creator.createLookupQuery(ROWDATA);

        // THEN - Should work without additional JSON
        assertThat(createdQuery.getLookupQuery()).isEqualTo("{\"key1\":\"val1\"}");
    }

    @Test
    public void testAdditionalJsonInvalidJson() {
        // GIVEN - Invalid JSON
        LookupRow lookupRow = getLookupRow(KEY_1);
        Configuration config = getConfiguration("POST");
        config.set(
                GenericJsonAndUrlQueryCreatorFactory.REQUEST_ADDITIONAL_BODY_JSON,
                "{invalid json}");

        // WHEN/THEN - Should throw IllegalArgumentException during factory creation
        assertThrows(
                IllegalArgumentException.class,
                () -> {
                    new GenericJsonAndUrlQueryCreatorFactory()
                            .createLookupQueryCreator(
                                    config, lookupRow, getTableContext(config, RESOLVED_SCHEMA));
                });
    }

    @Test
    public void testAdditionalJsonNotAppliedToGet() {
        // GIVEN - GET request with additional JSON
        LookupRow lookupRow = getLookupRow(KEY_1);
        Configuration config = getConfiguration("GET");
        config.set(
                GenericJsonAndUrlQueryCreatorFactory.REQUEST_ADDITIONAL_BODY_JSON, "{\"c\":789}");

        GenericJsonAndUrlQueryCreator creator =
                (GenericJsonAndUrlQueryCreator)
                        new GenericJsonAndUrlQueryCreatorFactory()
                                .createLookupQueryCreator(
                                        config,
                                        lookupRow,
                                        getTableContext(config, RESOLVED_SCHEMA));

        // WHEN
        var createdQuery = creator.createLookupQuery(ROWDATA);

        // THEN - Additional JSON should not affect GET requests (query params only)
        assertThat(createdQuery.getLookupQuery()).isEqualTo("key1=val1");
    }

    @Test
    public void testAdditionalJsonOverridesJoinKey() {
        // GIVEN - Additional JSON that tries to override a join key
        LookupRow lookupRow = getLookupRow(KEY_1);
        Configuration config = getConfiguration("POST");
        config.set(
                GenericJsonAndUrlQueryCreatorFactory.REQUEST_ADDITIONAL_BODY_JSON,
                "{\"key1\":\"override_value\",\"c\":789}");

        // WHEN/THEN - Should throw IllegalArgumentException
        IllegalArgumentException exception =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> {
                            new GenericJsonAndUrlQueryCreatorFactory()
                                    .createLookupQueryCreator(
                                            config,
                                            lookupRow,
                                            getTableContext(config, RESOLVED_SCHEMA));
                        });

        // Verify the error message
        assertThat(exception.getMessage())
                .contains(
                        "The http.request.additional-body-json option should not override join keys");
        assertThat(exception.getMessage())
                .contains(
                        "as join keys are expected to target different enrichments on a request basis");
        assertThat(exception.getMessage()).contains("key1");
    }

    @Test
    public void testAdditionalJsonOverridesMultipleJoinKeys() {
        // GIVEN - Additional JSON that tries to override multiple join keys
        LookupRow lookupRow = getLookupRow(KEY_1, KEY_2);
        Configuration config = getConfiguration("POST");
        // Set body fields to include both keys
        config.set(GenericJsonAndUrlQueryCreatorFactory.REQUEST_BODY_FIELDS, List.of(KEY_1, KEY_2));
        config.set(
                GenericJsonAndUrlQueryCreatorFactory.REQUEST_ADDITIONAL_BODY_JSON,
                "{\"key1\":\"override1\",\"key2\":\"override2\",\"c\":789}");

        // WHEN/THEN - Should throw IllegalArgumentException with all conflicting fields
        IllegalArgumentException exception =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> {
                            new GenericJsonAndUrlQueryCreatorFactory()
                                    .createLookupQueryCreator(
                                            config,
                                            lookupRow,
                                            getTableContext(config, RESOLVED_SCHEMA));
                        });

        // Verify the error message contains both fields
        assertThat(exception.getMessage())
                .contains(
                        "The http.request.additional-body-json option should not override join keys");
        assertThat(exception.getMessage())
                .contains(
                        "as join keys are expected to target different enrichments on a request basis");
        assertThat(exception.getMessage()).contains("Found conflicting fields:");
        assertThat(exception.getMessage()).contains("key1");
        assertThat(exception.getMessage()).contains("key2");
    }

    @Test
    public void testAdditionalJsonOverridesMultipleJoinKeysDifferentOrder() {
        // GIVEN - Additional JSON with fields in different order than body fields
        // Body fields: key1, key2
        // Additional JSON: key2, key1 (reversed order)
        LookupRow lookupRow = getLookupRow(KEY_1, KEY_2);
        Configuration config = getConfiguration("POST");
        // Set body fields: key1, key2
        config.set(GenericJsonAndUrlQueryCreatorFactory.REQUEST_BODY_FIELDS, List.of(KEY_1, KEY_2));
        // Additional JSON has reversed order: key2, key1
        config.set(
                GenericJsonAndUrlQueryCreatorFactory.REQUEST_ADDITIONAL_BODY_JSON,
                "{\"key2\":\"override2\",\"key1\":\"override1\",\"c\":789}");

        // WHEN/THEN - Should throw IllegalArgumentException with all conflicting fields
        IllegalArgumentException exception =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> {
                            new GenericJsonAndUrlQueryCreatorFactory()
                                    .createLookupQueryCreator(
                                            config,
                                            lookupRow,
                                            getTableContext(config, RESOLVED_SCHEMA));
                        });

        // Verify the error message contains both fields regardless of order
        assertThat(exception.getMessage())
                .contains(
                        "The http.request.additional-body-json option should not override join keys");
        assertThat(exception.getMessage())
                .contains(
                        "as join keys are expected to target different enrichments on a request basis");
        assertThat(exception.getMessage()).contains("Found conflicting fields:");
        assertThat(exception.getMessage()).contains("key1");
        assertThat(exception.getMessage()).contains("key2");
    }

    @Test
    public void testAdditionalJsonWithNoBodyFields() throws Exception {
        // GIVEN - Lookup key mapped to query param, not body field
        // This allows additional JSON to be the entire request body
        LookupRow lookupRow = getLookupRow(KEY_1);
        Configuration config = new Configuration();
        config.set(GenericJsonAndUrlQueryCreatorFactory.REQUEST_URL_MAP, urlParams);
        config.set(LOOKUP_METHOD, "POST");
        // Map lookup key to query param, not body field
        config.set(GenericJsonAndUrlQueryCreatorFactory.REQUEST_QUERY_PARAM_FIELDS, QUERY_PARAMS);
        // Don't set REQUEST_BODY_FIELDS - it will be empty (no body fields)
        config.set(
                GenericJsonAndUrlQueryCreatorFactory.REQUEST_ADDITIONAL_BODY_JSON,
                "{\"staticField\":\"staticValue\",\"count\":42,\"active\":true}");

        GenericJsonAndUrlQueryCreator creator =
                (GenericJsonAndUrlQueryCreator)
                        new GenericJsonAndUrlQueryCreatorFactory()
                                .createLookupQueryCreator(
                                        config,
                                        lookupRow,
                                        getTableContext(config, RESOLVED_SCHEMA));

        // WHEN
        var createdQuery = creator.createLookupQuery(ROWDATA);

        // THEN - Additional JSON should be used as the entire body
        String expectedJson = "{\"staticField\":\"staticValue\",\"count\":42,\"active\":true}";
        ObjectMapper mapper = new ObjectMapper();
        JsonNode expected = mapper.readTree(expectedJson);
        JsonNode actual = mapper.readTree(createdQuery.getLookupQuery());
        assertThat(actual).isEqualTo(expected);
        // Verify the lookup key is in query params, not body
        assertThat(createdQuery.getBodyBasedUrlQueryParameters()).isEqualTo(KEY_1 + "=" + VALUE);
    }

    @Test
    public void testAdditionalJsonOverridesBodyFieldFromUserScenario() {
        // GIVEN - User scenario: body field 'customerId' with additional JSON trying to override it
        LookupRow lookupRow = new LookupRow();
        lookupRow.addLookupEntry(
                new RowDataSingleValueLookupSchemaEntry(
                        "customerId",
                        RowData.createFieldGetter(DataTypes.STRING().getLogicalType(), 0)));
        lookupRow.setLookupPhysicalRowDataType(
                row(List.of(DataTypes.FIELD("customerId", DataTypes.STRING()))));

        Configuration config = new Configuration();
        config.set(GenericJsonAndUrlQueryCreatorFactory.REQUEST_URL_MAP, urlParams);
        config.set(LOOKUP_METHOD, "POST");
        config.set(GenericJsonAndUrlQueryCreatorFactory.REQUEST_BODY_FIELDS, List.of("customerId"));
        config.set(
                GenericJsonAndUrlQueryCreatorFactory.REQUEST_ADDITIONAL_BODY_JSON,
                "{\"customerId\":\"bbb\"}");

        // WHEN/THEN - Should throw IllegalArgumentException because additional JSON tries to
        // override body field
        IllegalArgumentException exception =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> {
                            new GenericJsonAndUrlQueryCreatorFactory()
                                    .createLookupQueryCreator(
                                            config,
                                            lookupRow,
                                            getTableContext(config, RESOLVED_SCHEMA));
                        });

        // Verify error message mentions the conflicting field
        assertThat(exception.getMessage()).contains("customerId");
        assertThat(exception.getMessage())
                .contains("http.request.additional-body-json option should not override join keys");
    }

    @Test
    public void testAdditionalJsonCaseSensitiveJoinKeyCheck() {
        // GIVEN - Additional JSON with different case than join key
        LookupRow lookupRow = getLookupRow(KEY_1); // key1
        Configuration config = getConfiguration("POST");
        // KEY1 (uppercase) should not conflict with key1 (lowercase) - case sensitive
        config.set(
                GenericJsonAndUrlQueryCreatorFactory.REQUEST_ADDITIONAL_BODY_JSON,
                "{\"KEY1\":\"value\",\"c\":789}");

        // WHEN - Should succeed because case is different
        GenericJsonAndUrlQueryCreator creator =
                (GenericJsonAndUrlQueryCreator)
                        new GenericJsonAndUrlQueryCreatorFactory()
                                .createLookupQueryCreator(
                                        config,
                                        lookupRow,
                                        getTableContext(config, RESOLVED_SCHEMA));

        // THEN - Should work fine
        var createdQuery = creator.createLookupQuery(ROWDATA);
        String lookupQuery = createdQuery.getLookupQuery();
        assertThat(lookupQuery).contains("key1");
        assertThat(lookupQuery).contains("KEY1");
    }

    private static void validateCreatedQueryForGet(LookupQueryInfo createdQuery) {
        // check there is no body params and we have the expected lookup query
        assertThat(createdQuery.getBodyBasedUrlQueryParameters()).isEmpty();
        assertThat(createdQuery.getLookupQuery()).isEqualTo(KEY_1 + "=" + VALUE);
    }

    private static void validateCreatedQueryForPutAndPost(LookupQueryInfo createdQuery) {
        // check we have the expected body params and lookup query
        assertThat(createdQuery.getBodyBasedUrlQueryParameters()).isEqualTo(KEY_1 + "=" + VALUE);
        assertThat(createdQuery.getLookupQuery())
                .isEqualTo("{\"" + KEY_1 + "\":\"" + VALUE + "\"}");
    }

    private static GenericRowData getRowData(int arity, String value) {
        var row = new GenericRowData(arity);
        row.setField(0, StringData.fromString(value));
        return row;
    }

    private static Configuration getConfiguration(String operation) {
        Configuration config = new Configuration();
        config.set(GenericJsonAndUrlQueryCreatorFactory.REQUEST_QUERY_PARAM_FIELDS, QUERY_PARAMS);
        if (!operation.equals("GET")) {
            // add the body content for PUT and POST
            config.set(GenericJsonAndUrlQueryCreatorFactory.REQUEST_BODY_FIELDS, QUERY_PARAMS);
        }
        config.set(GenericJsonAndUrlQueryCreatorFactory.REQUEST_URL_MAP, urlParams);
        config.set(LOOKUP_METHOD, operation);
        return config;
    }

    private static LookupRow getLookupRow(String... keys) {

        LookupRow lookupRow = new LookupRow();
        for (int keyNumber = 0; keyNumber < keys.length; keyNumber++) {
            lookupRow.addLookupEntry(
                    new RowDataSingleValueLookupSchemaEntry(
                            keys[keyNumber],
                            RowData.createFieldGetter(
                                    DataTypes.STRING().getLogicalType(), keyNumber)));
            lookupRow.setLookupPhysicalRowDataType(DATATYPE_1);
        }
        return lookupRow;
    }

    private static Row rowDataToRow(final RowData lookupRowData, final DataType rowType) {
        Preconditions.checkNotNull(lookupRowData);
        Preconditions.checkNotNull(rowType);

        final Row row = Row.withNames();
        final List<DataTypes.Field> rowFields = FieldsDataType.getFields(rowType);

        for (int idx = 0; idx < rowFields.size(); idx++) {
            final String fieldName = rowFields.get(idx).getName();
            final Object fieldValue = ((GenericRowData) lookupRowData).getField(idx);
            row.setField(fieldName, fieldValue);
        }
        return row;
    }
}
