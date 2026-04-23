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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.http.LookupQueryCreator;
import org.apache.flink.connector.http.table.lookup.HttpLookupConnectorOptions;
import org.apache.flink.connector.http.table.lookup.LookupQueryInfo;
import org.apache.flink.connector.http.table.lookup.LookupRow;
import org.apache.flink.connector.http.table.lookup.RowDataSingleValueLookupSchemaEntry;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.factories.DynamicTableFactory;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.apache.flink.connector.http.table.lookup.HttpLookupTableSourceFactory.row;
import static org.apache.flink.connector.http.table.lookup.querycreators.GenericJsonAndUrlQueryCreatorFactory.REQUEST_BODY_TEMPLATE;
import static org.apache.flink.connector.http.table.lookup.querycreators.GenericJsonAndUrlQueryCreatorFactory.REQUEST_QUERY_PARAM_FIELDS;
import static org.apache.flink.connector.http.table.lookup.querycreators.GenericJsonAndUrlQueryCreatorFactory.REQUEST_URL_MAP;
import static org.apache.flink.connector.http.table.lookup.querycreators.QueryCreatorUtils.getTableContext;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link GenericJsonAndUrlQueryCreatorFactory}. */
class GenericJsonAndUrlQueryCreatorFactoryTest {
    private Configuration config = new Configuration();

    private DynamicTableFactory.Context tableContext;

    @BeforeEach
    public void setUp() {
        CustomJsonFormatFactory.requiredOptionsWereUsed = false;
        this.tableContext =
                getTableContext(
                        this.config,
                        ResolvedSchema.of(Column.physical("key1", DataTypes.STRING())));
    }

    @Test
    public void lookupQueryInfoTestStr() {
        assertThat(CustomJsonFormatFactory.requiredOptionsWereUsed)
                .withFailMessage(
                        "CustomJsonFormat was not cleared, "
                                + "make sure `CustomJsonFormatFactory.requiredOptionsWereUsed"
                                + "= false` "
                                + "was called before this test execution.")
                .isFalse();

        this.config.setString("lookup-request.format", CustomJsonFormatFactory.IDENTIFIER);
        this.config.setString(
                String.format(
                        "lookup-request.format.%s.%s",
                        CustomJsonFormatFactory.IDENTIFIER,
                        CustomJsonFormatFactory.REQUIRED_OPTION),
                "optionValue");
        this.config.set(REQUEST_QUERY_PARAM_FIELDS, List.of("key1"));
        // with sync
        createUsingFactory(false);
        // with async
        createUsingFactory(true);
    }

    @Test
    public void lookupQueryInfoTestRequiredConfig() {
        GenericJsonAndUrlQueryCreatorFactory genericJsonAndUrlQueryCreatorFactory =
                new GenericJsonAndUrlQueryCreatorFactory();
        assertThatThrownBy(
                        () -> {
                            genericJsonAndUrlQueryCreatorFactory.createLookupQueryCreator(
                                    config, null, null);
                        })
                .isInstanceOf(RuntimeException.class);
    }

    private void createUsingFactory(boolean async) {
        this.config.set(HttpLookupConnectorOptions.ASYNC_POLLING, async);
        LookupRow lookupRow =
                new LookupRow()
                        .addLookupEntry(
                                new RowDataSingleValueLookupSchemaEntry(
                                        "key1",
                                        RowData.createFieldGetter(
                                                DataTypes.STRING().getLogicalType(), 0)));

        lookupRow.setLookupPhysicalRowDataType(
                row(List.of(DataTypes.FIELD("key1", DataTypes.STRING()))));
        LookupQueryCreator lookupQueryCreator =
                new GenericJsonAndUrlQueryCreatorFactory()
                        .createLookupQueryCreator(config, lookupRow, tableContext);
        GenericRowData lookupRowData = GenericRowData.of(StringData.fromString("val1"));

        LookupQueryInfo lookupQueryInfo = lookupQueryCreator.createLookupQuery(lookupRowData);
        assertThat(CustomJsonFormatFactory.requiredOptionsWereUsed).isTrue();
        assertThat(lookupQueryInfo.hasLookupQuery()).isTrue();
        assertThat(lookupQueryInfo.hasBodyBasedUrlQueryParameters()).isFalse();
        assertThat(lookupQueryInfo.hasPathBasedUrlParameters()).isFalse();
    }

    @Test
    void optionsTests() {
        GenericJsonAndUrlQueryCreatorFactory factory = new GenericJsonAndUrlQueryCreatorFactory();
        assertThat(factory.requiredOptions()).isEmpty();
        assertThat(factory.optionalOptions()).contains(REQUEST_QUERY_PARAM_FIELDS);
        assertThat(factory.optionalOptions()).contains(REQUEST_BODY_TEMPLATE);
        assertThat(factory.optionalOptions()).contains(REQUEST_URL_MAP);
    }

    @Test
    void testBodyTemplateWithSimplePlaceholders() {
        // GIVEN - Body template with simple field placeholders
        Configuration config = new Configuration();
        config.set(HttpLookupConnectorOptions.LOOKUP_METHOD, "POST");
        config.set(REQUEST_BODY_TEMPLATE, "{\"userId\":{{key1}},\"status\":\"active\"}");

        LookupRow lookupRow = new LookupRow();
        lookupRow.addLookupEntry(
                new RowDataSingleValueLookupSchemaEntry(
                        "key1", RowData.createFieldGetter(DataTypes.STRING().getLogicalType(), 0)));
        lookupRow.setLookupPhysicalRowDataType(
                row(List.of(DataTypes.FIELD("key1", DataTypes.STRING()))));

        // WHEN/THEN - Should succeed
        LookupQueryCreator creator =
                new GenericJsonAndUrlQueryCreatorFactory()
                        .createLookupQueryCreator(config, lookupRow, tableContext);
    }

    @Test
    void testValidationRejectsNullColumnName() {
        // GIVEN - Map with null column name
        Configuration config = new Configuration();
        config.set(HttpLookupConnectorOptions.LOOKUP_METHOD, "GET");
        java.util.Map<String, String> queryParamMap = new java.util.HashMap<>();
        queryParamMap.put(null, "qp1");
        config.set(
                GenericJsonAndUrlQueryCreatorFactory.REQUEST_QUERY_PARAM_FIELDS_WITH_KEY,
                queryParamMap);

        LookupRow lookupRow = new LookupRow();
        lookupRow.addLookupEntry(
                new RowDataSingleValueLookupSchemaEntry(
                        "key1", RowData.createFieldGetter(DataTypes.STRING().getLogicalType(), 0)));

        // WHEN/THEN - Should throw IllegalArgumentException
        assertThatThrownBy(
                        () ->
                                new GenericJsonAndUrlQueryCreatorFactory()
                                        .createLookupQueryCreator(config, lookupRow, tableContext))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Column name")
                .hasMessageContaining("cannot be null or empty");
    }

    @Test
    void testValidationRejectsEmptyColumnName() {
        // GIVEN - Map with empty column name
        Configuration config = new Configuration();
        config.set(HttpLookupConnectorOptions.LOOKUP_METHOD, "GET");
        java.util.Map<String, String> queryParamMap = new java.util.HashMap<>();
        queryParamMap.put("", "qp1");
        config.set(
                GenericJsonAndUrlQueryCreatorFactory.REQUEST_QUERY_PARAM_FIELDS_WITH_KEY,
                queryParamMap);

        LookupRow lookupRow = new LookupRow();
        lookupRow.addLookupEntry(
                new RowDataSingleValueLookupSchemaEntry(
                        "key1", RowData.createFieldGetter(DataTypes.STRING().getLogicalType(), 0)));

        // WHEN/THEN - Should throw IllegalArgumentException
        assertThatThrownBy(
                        () ->
                                new GenericJsonAndUrlQueryCreatorFactory()
                                        .createLookupQueryCreator(config, lookupRow, tableContext))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Column name")
                .hasMessageContaining("cannot be null or empty");
    }

    @Test
    void testValidationRejectsNullQueryParamKey() {
        // GIVEN - Map with null query param key
        Configuration config = new Configuration();
        config.set(HttpLookupConnectorOptions.LOOKUP_METHOD, "GET");
        java.util.Map<String, String> queryParamMap = new java.util.HashMap<>();
        queryParamMap.put("key1", null);
        config.set(
                GenericJsonAndUrlQueryCreatorFactory.REQUEST_QUERY_PARAM_FIELDS_WITH_KEY,
                queryParamMap);

        LookupRow lookupRow = new LookupRow();
        lookupRow.addLookupEntry(
                new RowDataSingleValueLookupSchemaEntry(
                        "key1", RowData.createFieldGetter(DataTypes.STRING().getLogicalType(), 0)));

        // WHEN/THEN - Should throw IllegalArgumentException
        assertThatThrownBy(
                        () ->
                                new GenericJsonAndUrlQueryCreatorFactory()
                                        .createLookupQueryCreator(config, lookupRow, tableContext))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Query parameter key for column 'key1'")
                .hasMessageContaining("cannot be null or empty");
    }

    @Test
    void testValidationRejectsEmptyQueryParamKey() {
        // GIVEN - Map with empty query param key
        Configuration config = new Configuration();
        config.set(HttpLookupConnectorOptions.LOOKUP_METHOD, "GET");
        java.util.Map<String, String> queryParamMap = new java.util.HashMap<>();
        queryParamMap.put("key1", "  ");
        config.set(
                GenericJsonAndUrlQueryCreatorFactory.REQUEST_QUERY_PARAM_FIELDS_WITH_KEY,
                queryParamMap);

        LookupRow lookupRow = new LookupRow();
        lookupRow.addLookupEntry(
                new RowDataSingleValueLookupSchemaEntry(
                        "key1", RowData.createFieldGetter(DataTypes.STRING().getLogicalType(), 0)));

        // WHEN/THEN - Should throw IllegalArgumentException
        assertThatThrownBy(
                        () ->
                                new GenericJsonAndUrlQueryCreatorFactory()
                                        .createLookupQueryCreator(config, lookupRow, tableContext))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Query parameter key for column 'key1'")
                .hasMessageContaining("cannot be null or empty");
    }

    @Test
    void testValidationRejectsConflictBetweenQueryParamKeyAndListFormat() {
        // GIVEN - REQUEST_QUERY_PARAM_FIELDS_WITH_KEY with query param key that conflicts with
        // REQUEST_QUERY_PARAM_FIELDS
        Configuration config = new Configuration();
        config.set(HttpLookupConnectorOptions.LOOKUP_METHOD, "GET");
        config.set(REQUEST_QUERY_PARAM_FIELDS, List.of("qp1", "qp2"));
        java.util.Map<String, String> queryParamMap = new java.util.HashMap<>();
        queryParamMap.put("key1", "qp1"); // qp1 is already in REQUEST_QUERY_PARAM_FIELDS
        config.set(
                GenericJsonAndUrlQueryCreatorFactory.REQUEST_QUERY_PARAM_FIELDS_WITH_KEY,
                queryParamMap);

        LookupRow lookupRow = new LookupRow();
        lookupRow.addLookupEntry(
                new RowDataSingleValueLookupSchemaEntry(
                        "key1", RowData.createFieldGetter(DataTypes.STRING().getLogicalType(), 0)));

        // WHEN/THEN - Should throw IllegalArgumentException
        assertThatThrownBy(
                        () ->
                                new GenericJsonAndUrlQueryCreatorFactory()
                                        .createLookupQueryCreator(config, lookupRow, tableContext))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Query parameter key 'qp1'")
                .hasMessageContaining("conflicts with existing columns");
    }

    @Test
    void testValidationRejectsConflictBetweenColumnNameAndListFormat() {
        // GIVEN - REQUEST_QUERY_PARAM_FIELDS_WITH_KEY with column name that conflicts with
        // REQUEST_QUERY_PARAM_FIELDS
        Configuration config = new Configuration();
        config.set(HttpLookupConnectorOptions.LOOKUP_METHOD, "GET");
        config.set(REQUEST_QUERY_PARAM_FIELDS, List.of("key1", "qp2"));
        java.util.Map<String, String> queryParamMap = new java.util.HashMap<>();
        queryParamMap.put("key1", "customParam"); // key1 is already in REQUEST_QUERY_PARAM_FIELDS
        config.set(
                GenericJsonAndUrlQueryCreatorFactory.REQUEST_QUERY_PARAM_FIELDS_WITH_KEY,
                queryParamMap);

        LookupRow lookupRow = new LookupRow();
        lookupRow.addLookupEntry(
                new RowDataSingleValueLookupSchemaEntry(
                        "key1", RowData.createFieldGetter(DataTypes.STRING().getLogicalType(), 0)));

        // WHEN/THEN - Should throw IllegalArgumentException
        assertThatThrownBy(
                        () ->
                                new GenericJsonAndUrlQueryCreatorFactory()
                                        .createLookupQueryCreator(config, lookupRow, tableContext))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Column name 'key1'")
                .hasMessageContaining("conflicts with existing columns");
    }

    @Test
    void testValidationRejectsDuplicateQueryParamKeys() {
        // GIVEN - Map with duplicate query param keys (different columns mapping to same key)
        Configuration config = new Configuration();
        config.set(HttpLookupConnectorOptions.LOOKUP_METHOD, "GET");
        java.util.Map<String, String> queryParamMap = new java.util.LinkedHashMap<>();
        queryParamMap.put("customerId", "id");
        queryParamMap.put("orderId", "id"); // Same key "id" used twice
        config.set(
                GenericJsonAndUrlQueryCreatorFactory.REQUEST_QUERY_PARAM_FIELDS_WITH_KEY,
                queryParamMap);

        LookupRow lookupRow = new LookupRow();
        lookupRow.addLookupEntry(
                new RowDataSingleValueLookupSchemaEntry(
                        "customerId",
                        RowData.createFieldGetter(DataTypes.STRING().getLogicalType(), 0)));
        lookupRow.addLookupEntry(
                new RowDataSingleValueLookupSchemaEntry(
                        "orderId",
                        RowData.createFieldGetter(DataTypes.STRING().getLogicalType(), 1)));

        // WHEN/THEN - Should throw IllegalArgumentException
        assertThatThrownBy(
                        () ->
                                new GenericJsonAndUrlQueryCreatorFactory()
                                        .createLookupQueryCreator(config, lookupRow, tableContext))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Duplicate query parameter key 'id'")
                .hasMessageContaining("must be unique");
    }

    @Test
    void testBodyTemplateWithNestedStructure() {
        // GIVEN - Body template with nested JSON structure
        Configuration config = new Configuration();
        config.set(HttpLookupConnectorOptions.LOOKUP_METHOD, "POST");
        config.set(
                REQUEST_BODY_TEMPLATE,
                "{\"user\":{\"id\":{{key1}},\"status\":\"active\"},\"metadata\":{\"version\":\"1.0\"}}");

        LookupRow lookupRow = new LookupRow();
        lookupRow.addLookupEntry(
                new RowDataSingleValueLookupSchemaEntry(
                        "key1", RowData.createFieldGetter(DataTypes.STRING().getLogicalType(), 0)));
        lookupRow.setLookupPhysicalRowDataType(
                row(List.of(DataTypes.FIELD("key1", DataTypes.STRING()))));

        // WHEN/THEN - Should succeed
        LookupQueryCreator creator =
                new GenericJsonAndUrlQueryCreatorFactory()
                        .createLookupQueryCreator(config, lookupRow, tableContext);
        assertThat(creator).isNotNull();
    }

    @Test
    void testBodyTemplateWithMultiplePlaceholders() {
        // GIVEN - Body template with multiple field placeholders
        Configuration config = new Configuration();
        config.set(HttpLookupConnectorOptions.LOOKUP_METHOD, "POST");
        config.set(
                REQUEST_BODY_TEMPLATE,
                "{\"userId\":{{key1}},\"userName\":{{key2}},\"status\":\"active\"}");

        LookupRow lookupRow = new LookupRow();
        lookupRow.addLookupEntry(
                new RowDataSingleValueLookupSchemaEntry(
                        "key1", RowData.createFieldGetter(DataTypes.STRING().getLogicalType(), 0)));
        lookupRow.addLookupEntry(
                new RowDataSingleValueLookupSchemaEntry(
                        "key2", RowData.createFieldGetter(DataTypes.STRING().getLogicalType(), 1)));
        lookupRow.setLookupPhysicalRowDataType(
                row(
                        List.of(
                                DataTypes.FIELD("key1", DataTypes.STRING()),
                                DataTypes.FIELD("key2", DataTypes.STRING()))));

        // WHEN/THEN - Should succeed
        LookupQueryCreator creator =
                new GenericJsonAndUrlQueryCreatorFactory()
                        .createLookupQueryCreator(config, lookupRow, tableContext);
        assertThat(creator).isNotNull();
    }

    @Test
    void testBodyTemplateWithOnlyLiterals() {
        // GIVEN - Body template with no placeholders (only literals)
        Configuration config = new Configuration();
        config.set(HttpLookupConnectorOptions.LOOKUP_METHOD, "POST");
        config.set(REQUEST_BODY_TEMPLATE, "{\"status\":\"active\",\"version\":\"1.0\"}");

        LookupRow lookupRow = new LookupRow();
        lookupRow.addLookupEntry(
                new RowDataSingleValueLookupSchemaEntry(
                        "key1", RowData.createFieldGetter(DataTypes.STRING().getLogicalType(), 0)));
        lookupRow.setLookupPhysicalRowDataType(
                row(List.of(DataTypes.FIELD("key1", DataTypes.STRING()))));

        // WHEN/THEN - Should succeed
        LookupQueryCreator creator =
                new GenericJsonAndUrlQueryCreatorFactory()
                        .createLookupQueryCreator(config, lookupRow, tableContext);
        assertThat(creator).isNotNull();
    }

    @Test
    void testValidationRejectsQueryParamsWithPost() {
        // GIVEN - POST request with query param fields (old format)
        Configuration config = new Configuration();
        config.set(HttpLookupConnectorOptions.LOOKUP_METHOD, "POST");
        config.set(REQUEST_QUERY_PARAM_FIELDS, List.of("key1"));

        LookupRow lookupRow = new LookupRow();
        lookupRow.addLookupEntry(
                new RowDataSingleValueLookupSchemaEntry(
                        "key1", RowData.createFieldGetter(DataTypes.STRING().getLogicalType(), 0)));

        // WHEN/THEN - Should throw IllegalArgumentException
        assertThatThrownBy(
                        () ->
                                new GenericJsonAndUrlQueryCreatorFactory()
                                        .createLookupQueryCreator(config, lookupRow, tableContext))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Query parameter configuration")
                .hasMessageContaining("can only be used with GET method");
    }

    @Test
    void testValidationRejectsQueryParamsWithKeyWithPost() {
        // GIVEN - POST request with query param fields with key (new format)
        Configuration config = new Configuration();
        config.set(HttpLookupConnectorOptions.LOOKUP_METHOD, "POST");
        java.util.Map<String, String> queryParamMap = new java.util.HashMap<>();
        queryParamMap.put("key1", "qp1");
        config.set(
                GenericJsonAndUrlQueryCreatorFactory.REQUEST_QUERY_PARAM_FIELDS_WITH_KEY,
                queryParamMap);

        LookupRow lookupRow = new LookupRow();
        lookupRow.addLookupEntry(
                new RowDataSingleValueLookupSchemaEntry(
                        "key1", RowData.createFieldGetter(DataTypes.STRING().getLogicalType(), 0)));

        // WHEN/THEN - Should throw IllegalArgumentException
        assertThatThrownBy(
                        () ->
                                new GenericJsonAndUrlQueryCreatorFactory()
                                        .createLookupQueryCreator(config, lookupRow, tableContext))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Query parameter configuration")
                .hasMessageContaining("can only be used with GET method");
    }

    @Test
    void testValidationRejectsQueryParamsWithPut() {
        // GIVEN - PUT request with query param fields
        Configuration config = new Configuration();
        config.set(HttpLookupConnectorOptions.LOOKUP_METHOD, "PUT");
        config.set(REQUEST_QUERY_PARAM_FIELDS, List.of("key1"));

        LookupRow lookupRow = new LookupRow();
        lookupRow.addLookupEntry(
                new RowDataSingleValueLookupSchemaEntry(
                        "key1", RowData.createFieldGetter(DataTypes.STRING().getLogicalType(), 0)));

        // WHEN/THEN - Should throw IllegalArgumentException
        assertThatThrownBy(
                        () ->
                                new GenericJsonAndUrlQueryCreatorFactory()
                                        .createLookupQueryCreator(config, lookupRow, tableContext))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Query parameter configuration")
                .hasMessageContaining("can only be used with GET method");
    }

    @Test
    void testValidationRejectsBodyTemplateWithGet() {
        // GIVEN - GET request with body template
        Configuration config = new Configuration();
        config.set(HttpLookupConnectorOptions.LOOKUP_METHOD, "GET");
        config.set(REQUEST_BODY_TEMPLATE, "{\"userId\":{{key1}}}");

        LookupRow lookupRow = new LookupRow();
        lookupRow.addLookupEntry(
                new RowDataSingleValueLookupSchemaEntry(
                        "key1", RowData.createFieldGetter(DataTypes.STRING().getLogicalType(), 0)));

        // WHEN/THEN - Should throw IllegalArgumentException
        assertThatThrownBy(
                        () ->
                                new GenericJsonAndUrlQueryCreatorFactory()
                                        .createLookupQueryCreator(config, lookupRow, tableContext))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Body template configuration")
                .hasMessageContaining("cannot be used with GET method");
    }

    @Test
    void testValidationAllowsQueryParamsWithGet() {
        // GIVEN - GET request with query param fields (should succeed)
        Configuration config = new Configuration();
        config.set(HttpLookupConnectorOptions.LOOKUP_METHOD, "GET");
        config.set(REQUEST_QUERY_PARAM_FIELDS, List.of("key1"));

        LookupRow lookupRow = new LookupRow();
        lookupRow.addLookupEntry(
                new RowDataSingleValueLookupSchemaEntry(
                        "key1", RowData.createFieldGetter(DataTypes.STRING().getLogicalType(), 0)));
        lookupRow.setLookupPhysicalRowDataType(
                row(List.of(DataTypes.FIELD("key1", DataTypes.STRING()))));

        // WHEN/THEN - Should succeed
        LookupQueryCreator creator =
                new GenericJsonAndUrlQueryCreatorFactory()
                        .createLookupQueryCreator(config, lookupRow, tableContext);
        assertThat(creator).isNotNull();
    }

    @Test
    void testValidationRejectsBodyTemplateWithDefaultGetMethod() {
        // GIVEN - No lookup-method specified (defaults to GET) with body template
        Configuration config = new Configuration();
        // Note: NOT setting LOOKUP_METHOD - should default to GET
        config.set(REQUEST_BODY_TEMPLATE, "{\"userId\":{{key1}}}");

        LookupRow lookupRow = new LookupRow();
        lookupRow.addLookupEntry(
                new RowDataSingleValueLookupSchemaEntry(
                        "key1", RowData.createFieldGetter(DataTypes.STRING().getLogicalType(), 0)));

        // WHEN/THEN - Should throw IllegalArgumentException because default GET cannot use body
        // template
        assertThatThrownBy(
                        () ->
                                new GenericJsonAndUrlQueryCreatorFactory()
                                        .createLookupQueryCreator(config, lookupRow, tableContext))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Body template configuration")
                .hasMessageContaining("cannot be used with GET method");
    }

    @Test
    void testValidationAllowsQueryParamsWithDefaultGetMethod() {
        // GIVEN - No lookup-method specified (defaults to GET) with query param fields
        Configuration config = new Configuration();
        // Note: NOT setting LOOKUP_METHOD - should default to GET
        config.set(REQUEST_QUERY_PARAM_FIELDS, List.of("key1"));

        LookupRow lookupRow = new LookupRow();
        lookupRow.addLookupEntry(
                new RowDataSingleValueLookupSchemaEntry(
                        "key1", RowData.createFieldGetter(DataTypes.STRING().getLogicalType(), 0)));
        lookupRow.setLookupPhysicalRowDataType(
                row(List.of(DataTypes.FIELD("key1", DataTypes.STRING()))));

        // WHEN/THEN - Should succeed because default GET allows query parameters
        LookupQueryCreator creator =
                new GenericJsonAndUrlQueryCreatorFactory()
                        .createLookupQueryCreator(config, lookupRow, tableContext);
        assertThat(creator).isNotNull();
    }

    @Test
    void testValidationAllowsBodyTemplateWithPost() {
        // GIVEN - POST request with body template (should succeed)
        Configuration config = new Configuration();
        config.set(HttpLookupConnectorOptions.LOOKUP_METHOD, "POST");
        config.set(REQUEST_BODY_TEMPLATE, "{\"userId\":{{key1}}}");

        LookupRow lookupRow = new LookupRow();
        lookupRow.addLookupEntry(
                new RowDataSingleValueLookupSchemaEntry(
                        "key1", RowData.createFieldGetter(DataTypes.STRING().getLogicalType(), 0)));
        lookupRow.setLookupPhysicalRowDataType(
                row(List.of(DataTypes.FIELD("key1", DataTypes.STRING()))));

        // WHEN/THEN - Should succeed
        LookupQueryCreator creator =
                new GenericJsonAndUrlQueryCreatorFactory()
                        .createLookupQueryCreator(config, lookupRow, tableContext);
        assertThat(creator).isNotNull();
    }

    @Test
    void testValidationAllowsBodyTemplateWithPut() {
        // GIVEN - PUT request with body template (should succeed)
        Configuration config = new Configuration();
        config.set(HttpLookupConnectorOptions.LOOKUP_METHOD, "PUT");
        config.set(REQUEST_BODY_TEMPLATE, "{\"userId\":{{key1}}}");

        LookupRow lookupRow = new LookupRow();
        lookupRow.addLookupEntry(
                new RowDataSingleValueLookupSchemaEntry(
                        "key1", RowData.createFieldGetter(DataTypes.STRING().getLogicalType(), 0)));
        lookupRow.setLookupPhysicalRowDataType(
                row(List.of(DataTypes.FIELD("key1", DataTypes.STRING()))));

        // WHEN/THEN - Should succeed
        LookupQueryCreator creator =
                new GenericJsonAndUrlQueryCreatorFactory()
                        .createLookupQueryCreator(config, lookupRow, tableContext);
        assertThat(creator).isNotNull();
    }

    @Test
    void testNoBodyTemplateForPostReturnsEmptyJson() {
        // GIVEN - POST request with no body template
        Configuration config = new Configuration();
        config.set(HttpLookupConnectorOptions.LOOKUP_METHOD, "POST");
        // No REQUEST_BODY_TEMPLATE set

        LookupRow lookupRow = new LookupRow();
        lookupRow.addLookupEntry(
                new RowDataSingleValueLookupSchemaEntry(
                        "key1", RowData.createFieldGetter(DataTypes.STRING().getLogicalType(), 0)));
        lookupRow.setLookupPhysicalRowDataType(
                row(List.of(DataTypes.FIELD("key1", DataTypes.STRING()))));

        // WHEN/THEN - Should succeed and return empty JSON object
        LookupQueryCreator creator =
                new GenericJsonAndUrlQueryCreatorFactory()
                        .createLookupQueryCreator(config, lookupRow, tableContext);
        assertThat(creator).isNotNull();
    }
}
