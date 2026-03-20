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
import org.apache.flink.types.Row;

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
import static org.apache.flink.connector.http.table.lookup.querycreators.GenericJsonAndUrlQueryCreatorFactory.REQUEST_BODY_TEMPLATE;
import static org.apache.flink.connector.http.table.lookup.querycreators.GenericJsonAndUrlQueryCreatorFactory.REQUEST_QUERY_PARAM_FIELDS;
import static org.apache.flink.connector.http.table.lookup.querycreators.GenericJsonAndUrlQueryCreatorFactory.REQUEST_URL_MAP;
import static org.apache.flink.connector.http.table.lookup.querycreators.QueryCreatorUtils.getTableContext;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link GenericJsonAndUrlQueryCreator}. */
class GenericJsonAndUrlQueryCreatorTest {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String KEY_1 = "key1";
    private static final String KEY_2 = "key2";
    private static final String VALUE = "val1";
    private static final List<String> QUERY_PARAMS = List.of(KEY_1);
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
    }

    @Test
    public void createLookupQueryTest() {
        // GIVEN
        LookupRow lookupRow = getLookupRow(KEY_1, KEY_2);
        Configuration config = new Configuration();
        config.set(REQUEST_QUERY_PARAM_FIELDS, QUERY_PARAMS);
        config.set(REQUEST_URL_MAP, urlParams);
        config.set(LOOKUP_METHOD, "POST");
        lookupRow.setLookupPhysicalRowDataType(DATATYPE_1_2);
        GenericJsonAndUrlQueryCreator genericJsonAndUrlQueryCreator =
                (GenericJsonAndUrlQueryCreator)
                        new GenericJsonAndUrlQueryCreatorFactory()
                                .createLookupQueryCreator(
                                        config,
                                        lookupRow,
                                        getTableContext(config, RESOLVED_SCHEMA));
        // WHEN
        GenericRowData lookupRowData =
                GenericRowData.of(StringData.fromString("val1"), StringData.fromString("val2"));
        LookupQueryInfo createdQuery =
                genericJsonAndUrlQueryCreator.createLookupQuery(lookupRowData);
        // THEN
        assertThat(createdQuery.getLookupQuery()).isEqualTo("");
    }

    @Test
    public void failSerializationOpenTest() {
        // GIVEN
        LookupRow lookupRow = getLookupRow(KEY_1);
        lookupRow.setLookupPhysicalRowDataType(DATATYPE_1);
        GenericJsonAndUrlQueryCreator genericJsonAndUrlQueryCreator =
                (GenericJsonAndUrlQueryCreator)
                        new GenericJsonAndUrlQueryCreatorFactory()
                                .createLookupQueryCreator(
                                        getConfiguration("POST"),
                                        lookupRow,
                                        getTableContext(getConfiguration("POST"), RESOLVED_SCHEMA));
        // Mock a failing serialization schema
        SerializationSchema<RowData> failingSchema =
                new SerializationSchema<RowData>() {
                    @Override
                    public void open(InitializationContext context) throws Exception {
                        throw new Exception("Intentional failure");
                    }

                    @Override
                    public byte[] serialize(RowData element) {
                        return new byte[0];
                    }
                };
        genericJsonAndUrlQueryCreator.setSerializationSchema(failingSchema);
        // WHEN/THEN
        assertThatThrownBy(() -> genericJsonAndUrlQueryCreator.createLookupQuery(ROWDATA))
                .isInstanceOf(RuntimeException.class);
    }

    @Test
    void convertToQueryParametersUnsupportedEncodingTest() {
        // GIVEN
        PersonBean person = new PersonBean("aaa", "bbb");
        JsonNode personNode;
        try {
            personNode = OBJECT_MAPPER.readTree(OBJECT_MAPPER.writeValueAsString(person));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        // WHEN/THEN
        assertThatThrownBy(
                        () ->
                                GenericJsonAndUrlQueryCreator.convertToQueryParameters(
                                        (ObjectNode) personNode, "bad encoding"))
                .isInstanceOf(RuntimeException.class);
    }

    @Test
    void rowDataToRowTest() {
        // GIVEN
        GenericRowData rowData = GenericRowData.of(StringData.fromString("val1"));
        DataType dataType = row(List.of(DataTypes.FIELD(KEY_1, DataTypes.STRING())));
        // WHEN
        Row row = GenericJsonAndUrlQueryCreator.rowDataToRow(rowData, dataType);
        // THEN
        assertThat(row.getField(KEY_1).toString()).isEqualTo("val1");
    }

    @Test
    public void testBodyTemplateWithSimplePlaceholder() throws Exception {
        // GIVEN - Body template with simple field placeholder
        Configuration config = new Configuration();
        config.set(LOOKUP_METHOD, "POST");
        config.set(REQUEST_BODY_TEMPLATE, "{\"userId\":<key1>,\"status\":\"active\"}");

        LookupRow lookupRow = getLookupRow(KEY_1);
        lookupRow.setLookupPhysicalRowDataType(DATATYPE_1);

        GenericJsonAndUrlQueryCreator creator =
                (GenericJsonAndUrlQueryCreator)
                        new GenericJsonAndUrlQueryCreatorFactory()
                                .createLookupQueryCreator(
                                        config,
                                        lookupRow,
                                        getTableContext(config, RESOLVED_SCHEMA));

        // WHEN
        LookupQueryInfo createdQuery = creator.createLookupQuery(ROWDATA);

        // THEN
        JsonNode actual = OBJECT_MAPPER.readTree(createdQuery.getLookupQuery());
        assertThat(actual.get("userId").asText()).isEqualTo("val1");
        assertThat(actual.get("status").asText()).isEqualTo("active");
    }

    @Test
    public void testBodyTemplateWithNestedStructure() throws Exception {
        // GIVEN - Body template with nested JSON structure
        Configuration config = new Configuration();
        config.set(LOOKUP_METHOD, "POST");
        config.set(
                REQUEST_BODY_TEMPLATE,
                "{\"user\":{\"id\":<key1>,\"status\":\"active\"},\"metadata\":{\"version\":\"1.0\"}}");

        LookupRow lookupRow = getLookupRow(KEY_1);
        lookupRow.setLookupPhysicalRowDataType(DATATYPE_1);

        GenericJsonAndUrlQueryCreator creator =
                (GenericJsonAndUrlQueryCreator)
                        new GenericJsonAndUrlQueryCreatorFactory()
                                .createLookupQueryCreator(
                                        config,
                                        lookupRow,
                                        getTableContext(config, RESOLVED_SCHEMA));

        // WHEN
        LookupQueryInfo createdQuery = creator.createLookupQuery(ROWDATA);

        // THEN
        JsonNode actual = OBJECT_MAPPER.readTree(createdQuery.getLookupQuery());
        assertThat(actual.get("user").get("id").asText()).isEqualTo("val1");
        assertThat(actual.get("user").get("status").asText()).isEqualTo("active");
        assertThat(actual.get("metadata").get("version").asText()).isEqualTo("1.0");
    }

    @Test
    public void testBodyTemplateWithMultiplePlaceholders() throws Exception {
        // GIVEN - Body template with multiple field placeholders
        Configuration config = new Configuration();
        config.set(LOOKUP_METHOD, "POST");
        config.set(
                REQUEST_BODY_TEMPLATE,
                "{\"userId\":<key1>,\"userName\":<key2>,\"status\":\"active\"}");

        LookupRow lookupRow = getLookupRow(KEY_1, KEY_2);
        lookupRow.setLookupPhysicalRowDataType(DATATYPE_1_2);

        GenericJsonAndUrlQueryCreator creator =
                (GenericJsonAndUrlQueryCreator)
                        new GenericJsonAndUrlQueryCreatorFactory()
                                .createLookupQueryCreator(
                                        config,
                                        lookupRow,
                                        getTableContext(config, RESOLVED_SCHEMA));

        // WHEN
        GenericRowData lookupRowData =
                GenericRowData.of(StringData.fromString("val1"), StringData.fromString("val2"));
        LookupQueryInfo createdQuery = creator.createLookupQuery(lookupRowData);

        // THEN
        JsonNode actual = OBJECT_MAPPER.readTree(createdQuery.getLookupQuery());
        assertThat(actual.get("userId").asText()).isEqualTo("val1");
        assertThat(actual.get("userName").asText()).isEqualTo("val2");
        assertThat(actual.get("status").asText()).isEqualTo("active");
    }

    @Test
    public void testBodyTemplateWithOnlyLiterals() throws Exception {
        // GIVEN - Body template with no placeholders (only literals)
        Configuration config = new Configuration();
        config.set(LOOKUP_METHOD, "POST");
        config.set(REQUEST_BODY_TEMPLATE, "{\"status\":\"active\",\"version\":\"1.0\"}");

        LookupRow lookupRow = getLookupRow(KEY_1);
        lookupRow.setLookupPhysicalRowDataType(DATATYPE_1);

        GenericJsonAndUrlQueryCreator creator =
                (GenericJsonAndUrlQueryCreator)
                        new GenericJsonAndUrlQueryCreatorFactory()
                                .createLookupQueryCreator(
                                        config,
                                        lookupRow,
                                        getTableContext(config, RESOLVED_SCHEMA));

        // WHEN
        LookupQueryInfo createdQuery = creator.createLookupQuery(ROWDATA);

        // THEN
        JsonNode actual = OBJECT_MAPPER.readTree(createdQuery.getLookupQuery());
        assertThat(actual.get("status").asText()).isEqualTo("active");
        assertThat(actual.get("version").asText()).isEqualTo("1.0");
    }

    @Test
    public void testBodyTemplateNotAppliedToGet() {
        // GIVEN - GET request with body template (should be ignored)
        Configuration config = new Configuration();
        config.set(LOOKUP_METHOD, "GET");
        config.set(REQUEST_QUERY_PARAM_FIELDS, QUERY_PARAMS);
        config.set(REQUEST_BODY_TEMPLATE, "{\"userId\":<key1>}");

        LookupRow lookupRow = getLookupRow(KEY_1);
        lookupRow.setLookupPhysicalRowDataType(DATATYPE_1);

        GenericJsonAndUrlQueryCreator creator =
                (GenericJsonAndUrlQueryCreator)
                        new GenericJsonAndUrlQueryCreatorFactory()
                                .createLookupQueryCreator(
                                        config,
                                        lookupRow,
                                        getTableContext(config, RESOLVED_SCHEMA));

        // WHEN
        LookupQueryInfo createdQuery = creator.createLookupQuery(ROWDATA);

        // THEN - Should be query params, not body
        assertThat(createdQuery.getLookupQuery()).isEqualTo("key1=val1");
    }

    @Test
    public void testNoBodyTemplateReturnsEmptyJson() {
        // GIVEN - POST request with no body template
        Configuration config = new Configuration();
        config.set(LOOKUP_METHOD, "POST");
        // No REQUEST_BODY_TEMPLATE set

        LookupRow lookupRow = getLookupRow(KEY_1);
        lookupRow.setLookupPhysicalRowDataType(DATATYPE_1);

        GenericJsonAndUrlQueryCreator creator =
                (GenericJsonAndUrlQueryCreator)
                        new GenericJsonAndUrlQueryCreatorFactory()
                                .createLookupQueryCreator(
                                        config,
                                        lookupRow,
                                        getTableContext(config, RESOLVED_SCHEMA));

        // WHEN
        LookupQueryInfo createdQuery = creator.createLookupQuery(ROWDATA);

        // THEN
        assertThat(createdQuery.getLookupQuery()).isEqualTo("");
    }

    @Test
    public void testBodyTemplateWithInvalidPlaceholder() {
        // GIVEN - Body template with placeholder for non-existent field
        Configuration config = new Configuration();
        config.set(LOOKUP_METHOD, "POST");
        config.set(REQUEST_BODY_TEMPLATE, "{\"userId\":<nonExistentField>}");

        LookupRow lookupRow = getLookupRow(KEY_1);
        lookupRow.setLookupPhysicalRowDataType(DATATYPE_1);

        GenericJsonAndUrlQueryCreator creator =
                (GenericJsonAndUrlQueryCreator)
                        new GenericJsonAndUrlQueryCreatorFactory()
                                .createLookupQueryCreator(
                                        config,
                                        lookupRow,
                                        getTableContext(config, RESOLVED_SCHEMA));

        // WHEN/THEN
        assertThatThrownBy(() -> creator.createLookupQuery(ROWDATA))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("nonExistentField")
                .hasMessageContaining("does not exist");
    }

    @Test
    public void testBodyTemplateWithComplexNestedStructureAndTimestamps() throws Exception {
        // GIVEN - Complex template with primitives, arrays, nested objects, timestamps, and
        // literals
        Configuration config = new Configuration();
        config.set(LOOKUP_METHOD, "POST");

        // Template maps top-level fields to nested structure with literals
        String template =
                "{"
                        + "\"obj1\":{"
                        + "\"nestedString\":<topString>,"
                        + "\"nestedInt\":<topInt>,"
                        + "\"nestedBool\":<topBool>,"
                        + "\"nestedTimestamp\":<topTimestamp>,"
                        + "\"nestedStringArray\":<topStringArray>,"
                        + "\"nestedArrayOfObjects\":<topArrayOfObjects>,"
                        + "\"literalString\":\"constantValue\","
                        + "\"literalInt\":42,"
                        + "\"literalBool\":true,"
                        + "\"literalTimestamp\":\"2024-01-01T00:00:00Z\","
                        + "\"literalArray\":[\"lit1\",\"lit2\"],"
                        + "\"literalObject\":{\"key\":\"value\"}"
                        + "}"
                        + "}";
        config.set(REQUEST_BODY_TEMPLATE, template);

        // Create lookup row with all field types including timestamp
        DataType complexDataType =
                row(
                        List.of(
                                DataTypes.FIELD("topString", DataTypes.STRING()),
                                DataTypes.FIELD("topInt", DataTypes.INT()),
                                DataTypes.FIELD("topBool", DataTypes.BOOLEAN()),
                                DataTypes.FIELD("topTimestamp", DataTypes.TIMESTAMP(3)),
                                DataTypes.FIELD(
                                        "topStringArray", DataTypes.ARRAY(DataTypes.STRING())),
                                DataTypes.FIELD(
                                        "topArrayOfObjects",
                                        DataTypes.ARRAY(
                                                DataTypes.ROW(
                                                        DataTypes.FIELD("id", DataTypes.INT()),
                                                        DataTypes.FIELD(
                                                                "name", DataTypes.STRING()))))));

        LookupRow lookupRow = new LookupRow();
        lookupRow.addLookupEntry(
                new RowDataSingleValueLookupSchemaEntry(
                        "topString",
                        RowData.createFieldGetter(DataTypes.STRING().getLogicalType(), 0)));
        lookupRow.addLookupEntry(
                new RowDataSingleValueLookupSchemaEntry(
                        "topInt", RowData.createFieldGetter(DataTypes.INT().getLogicalType(), 1)));
        lookupRow.addLookupEntry(
                new RowDataSingleValueLookupSchemaEntry(
                        "topBool",
                        RowData.createFieldGetter(DataTypes.BOOLEAN().getLogicalType(), 2)));
        lookupRow.addLookupEntry(
                new RowDataSingleValueLookupSchemaEntry(
                        "topTimestamp",
                        RowData.createFieldGetter(DataTypes.TIMESTAMP(3).getLogicalType(), 3)));
        lookupRow.addLookupEntry(
                new RowDataSingleValueLookupSchemaEntry(
                        "topStringArray",
                        RowData.createFieldGetter(
                                DataTypes.ARRAY(DataTypes.STRING()).getLogicalType(), 4)));
        lookupRow.addLookupEntry(
                new RowDataSingleValueLookupSchemaEntry(
                        "topArrayOfObjects",
                        RowData.createFieldGetter(
                                DataTypes.ARRAY(
                                                DataTypes.ROW(
                                                        DataTypes.FIELD("id", DataTypes.INT()),
                                                        DataTypes.FIELD(
                                                                "name", DataTypes.STRING())))
                                        .getLogicalType(),
                                5)));
        lookupRow.setLookupPhysicalRowDataType(complexDataType);

        ResolvedSchema resolvedSchema =
                ResolvedSchema.of(
                        Column.physical("topString", DataTypes.STRING()),
                        Column.physical("topInt", DataTypes.INT()),
                        Column.physical("topBool", DataTypes.BOOLEAN()),
                        Column.physical("topTimestamp", DataTypes.TIMESTAMP(3)),
                        Column.physical("topStringArray", DataTypes.ARRAY(DataTypes.STRING())),
                        Column.physical(
                                "topArrayOfObjects",
                                DataTypes.ARRAY(
                                        DataTypes.ROW(
                                                DataTypes.FIELD("id", DataTypes.INT()),
                                                DataTypes.FIELD("name", DataTypes.STRING())))));

        GenericJsonAndUrlQueryCreator creator =
                (GenericJsonAndUrlQueryCreator)
                        new GenericJsonAndUrlQueryCreatorFactory()
                                .createLookupQueryCreator(
                                        config, lookupRow, getTableContext(config, resolvedSchema));

        // Create test data with actual values
        org.apache.flink.table.data.GenericArrayData stringArrayData =
                new org.apache.flink.table.data.GenericArrayData(
                        new Object[] {
                            StringData.fromString("arr1"),
                            StringData.fromString("arr2"),
                            StringData.fromString("arr3")
                        });

        GenericRowData obj1 = GenericRowData.of(1, StringData.fromString("Object1"));
        GenericRowData obj2 = GenericRowData.of(2, StringData.fromString("Object2"));
        org.apache.flink.table.data.GenericArrayData arrayOfObjectsData =
                new org.apache.flink.table.data.GenericArrayData(new Object[] {obj1, obj2});

        // Create timestamp data (milliseconds since epoch for 2023-06-15T10:30:00Z)
        org.apache.flink.table.data.TimestampData timestampData =
                org.apache.flink.table.data.TimestampData.fromEpochMillis(1686826200000L);

        GenericRowData rowData =
                GenericRowData.of(
                        StringData.fromString("testString"),
                        123,
                        true,
                        timestampData,
                        stringArrayData,
                        arrayOfObjectsData);

        // WHEN
        LookupQueryInfo createdQuery = creator.createLookupQuery(rowData);

        // THEN
        JsonNode actual = OBJECT_MAPPER.readTree(createdQuery.getLookupQuery());

        // Verify nested structure exists
        assertThat(actual.has("obj1")).isTrue();
        JsonNode obj1Node = actual.get("obj1");

        // Verify mapped fields from top-level to nested
        assertThat(obj1Node.get("nestedString").asText()).isEqualTo("testString");
        assertThat(obj1Node.get("nestedInt").asInt()).isEqualTo(123);
        assertThat(obj1Node.get("nestedBool").asBoolean()).isTrue();

        // Verify timestamp mapping
        assertThat(obj1Node.has("nestedTimestamp")).isTrue();
        String timestampStr = obj1Node.get("nestedTimestamp").asText();
        assertThat(timestampStr).contains("2023-06-15");

        // Verify array mapping
        JsonNode nestedArray = obj1Node.get("nestedStringArray");
        assertThat(nestedArray.isArray()).isTrue();
        assertThat(nestedArray.size()).isEqualTo(3);
        assertThat(nestedArray.get(0).asText()).isEqualTo("arr1");
        assertThat(nestedArray.get(1).asText()).isEqualTo("arr2");
        assertThat(nestedArray.get(2).asText()).isEqualTo("arr3");

        // Verify array of objects mapping
        JsonNode nestedObjArray = obj1Node.get("nestedArrayOfObjects");
        assertThat(nestedObjArray.isArray()).isTrue();
        assertThat(nestedObjArray.size()).isEqualTo(2);
        assertThat(nestedObjArray.get(0).get("id").asInt()).isEqualTo(1);
        assertThat(nestedObjArray.get(0).get("name").asText()).isEqualTo("Object1");
        assertThat(nestedObjArray.get(1).get("id").asInt()).isEqualTo(2);
        assertThat(nestedObjArray.get(1).get("name").asText()).isEqualTo("Object2");

        // Verify literals are preserved
        assertThat(obj1Node.get("literalString").asText()).isEqualTo("constantValue");
        assertThat(obj1Node.get("literalInt").asInt()).isEqualTo(42);
        assertThat(obj1Node.get("literalBool").asBoolean()).isTrue();
        assertThat(obj1Node.get("literalTimestamp").asText()).isEqualTo("2024-01-01T00:00:00Z");

        JsonNode literalArray = obj1Node.get("literalArray");
        assertThat(literalArray.isArray()).isTrue();
        assertThat(literalArray.size()).isEqualTo(2);
        assertThat(literalArray.get(0).asText()).isEqualTo("lit1");
        assertThat(literalArray.get(1).asText()).isEqualTo("lit2");

        JsonNode literalObject = obj1Node.get("literalObject");
        assertThat(literalObject.get("key").asText()).isEqualTo("value");
    }

    // Helper methods
    private static GenericRowData getRowData(int numFields, String value) {
        if (numFields == 1) {
            return GenericRowData.of(StringData.fromString(value));
        } else if (numFields == 2) {
            return GenericRowData.of(StringData.fromString(value), StringData.fromString(value));
        }
        throw new IllegalArgumentException("Unsupported number of fields: " + numFields);
    }

    private LookupRow getLookupRow(String... keys) {
        LookupRow lookupRow = new LookupRow();
        for (int i = 0; i < keys.length; i++) {
            lookupRow.addLookupEntry(
                    new RowDataSingleValueLookupSchemaEntry(
                            keys[i],
                            RowData.createFieldGetter(DataTypes.STRING().getLogicalType(), i)));
        }
        return lookupRow;
    }

    private Configuration getConfiguration(String operation) {
        Configuration config = new Configuration();
        config.set(REQUEST_QUERY_PARAM_FIELDS, QUERY_PARAMS);
        config.set(REQUEST_URL_MAP, urlParams);
        config.set(LOOKUP_METHOD, operation);
        return config;
    }

    private void validateCreatedQueryForGet(LookupQueryInfo createdQuery) {
        assertThat(createdQuery.hasLookupQuery()).isTrue();
        assertThat(createdQuery.getLookupQuery()).isEqualTo("key1=val1");
        assertThat(createdQuery.hasBodyBasedUrlQueryParameters()).isFalse();
        assertThat(createdQuery.hasPathBasedUrlParameters()).isTrue();
    }

    private void validateCreatedQueryForPutAndPost(LookupQueryInfo createdQuery) {
        // When no template is provided, body is empty (no body sent)
        assertThat(createdQuery.hasLookupQuery()).isFalse();
        assertThat(createdQuery.getLookupQuery()).isEqualTo("");
        assertThat(createdQuery.hasBodyBasedUrlQueryParameters()).isTrue();
        assertThat(createdQuery.hasPathBasedUrlParameters()).isTrue();
    }
}
