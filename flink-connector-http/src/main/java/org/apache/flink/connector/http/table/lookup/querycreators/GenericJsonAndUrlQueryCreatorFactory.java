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

package org.apache.flink.connector.http.table.lookup.querycreators;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.http.LookupQueryCreator;
import org.apache.flink.connector.http.LookupQueryCreatorFactory;
import org.apache.flink.connector.http.table.lookup.LookupRow;
import org.apache.flink.connector.http.utils.SynchronizedSerializationSchema;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.SerializationFormatFactory;
import org.apache.flink.table.types.DataType;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.configuration.ConfigOptions.key;
import static org.apache.flink.connector.http.table.lookup.HttpLookupConnectorOptions.ASYNC_POLLING;
import static org.apache.flink.connector.http.table.lookup.HttpLookupConnectorOptions.LOOKUP_METHOD;
import static org.apache.flink.connector.http.table.lookup.HttpLookupConnectorOptions.LOOKUP_REQUEST_FORMAT;

/**
 * Generic JSON and url query creator factory defined configuration to define the columns to be.
 *
 * <ol>
 *   <li>List of column names to be included in the query params
 *   <li>Map of templated uri segment names to column names
 *   <li>Body template with placeholders for dynamic values
 * </ol>
 */
@SuppressWarnings({"checkstyle:RegexpSingleline", "checkstyle:LineLength"})
public class GenericJsonAndUrlQueryCreatorFactory implements LookupQueryCreatorFactory {
    private static final long serialVersionUID = 1L;

    public static final String ID = "http-generic-json-url";

    public static final ConfigOption<List<String>> REQUEST_QUERY_PARAM_FIELDS =
            key("http.request.query-param-fields")
                    .stringType()
                    .asList()
                    .defaultValues() // default to empty list so we do not need to check for null
                    .withDescription(
                            "The names of the fields that will be mapped to query parameters."
                                    + " The parameters are separated by semicolons,"
                                    + " such as 'param1;param2'.");

    public static final ConfigOption<Map<String, String>> REQUEST_URL_MAP =
            ConfigOptions.key("http.request.url-map")
                    .mapType()
                    .noDefaultValue()
                    .withDescription(
                            "The map of insert names to column names used"
                                    + "as url segments. Parses a string as a map of strings. "
                                    + "<br>"
                                    + "For example if there are table columns called customerId"
                                    + " and orderId, then specifying value customerId:cid1,orderID:oid"
                                    + " and a url of https://myendpoint/customers/{cid}/orders/{oid}"
                                    + " will mean that the url used for the lookup query will"
                                    + " dynamically pickup the values for customerId, orderId"
                                    + " and use them in the url."
                                    + "<br>Notes<br>"
                                    + "The expected format of the map is:"
                                    + "<br>"
                                    + " key1:value1,key2:value2");

    public static final ConfigOption<String> REQUEST_BODY_TEMPLATE =
            key("http.request.body-template")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "A JSON template string for the request body. Use placeholders like {{fieldName}} "
                                    + "to reference top-level fields from the lookup row. The template can contain "
                                    + "nested structures with hardcoded literals and dynamic field values. "
                                    + "Example: '{\"Id\":{{customerId}},\"Orders\":{\"orderId\":{{topOrderId}},\"version\":\"1.0\"}}'.");

    @Override
    public LookupQueryCreator createLookupQueryCreator(
            final ReadableConfig readableConfig,
            final LookupRow lookupRow,
            final DynamicTableFactory.Context dynamicTableFactoryContext) {
        final String httpMethod = readableConfig.get(LOOKUP_METHOD);
        final String formatIdentifier = readableConfig.get(LOOKUP_REQUEST_FORMAT);
        // get the information from config
        final List<String> requestQueryParamsFields =
                readableConfig.get(REQUEST_QUERY_PARAM_FIELDS);
        Map<String, String> requestUrlMap = readableConfig.get(REQUEST_URL_MAP);
        String bodyTemplate = readableConfig.getOptional(REQUEST_BODY_TEMPLATE).orElse(null);

        final SerializationSchema<RowData> jsonSerializationSchema =
                createSerializationSchema(
                        formatIdentifier, readableConfig, dynamicTableFactoryContext, lookupRow);

        // create using config parameter values and specify serialization
        // schema from json format.
        return new GenericJsonAndUrlQueryCreator(
                httpMethod,
                jsonSerializationSchema,
                requestQueryParamsFields,
                requestUrlMap,
                bodyTemplate,
                lookupRow);
    }

    @Override
    public String factoryIdentifier() {
        return ID;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Set.of();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return Set.of(REQUEST_QUERY_PARAM_FIELDS, REQUEST_URL_MAP, REQUEST_BODY_TEMPLATE);
    }

    /**
     * Creates a serialization schema for converting RowData to JSON.
     *
     * <p>Note: While this is a lookup operation (typically associated with deserialization), we use
     * a SerializationSchema here because we need to convert Flink's internal RowData representation
     * to JSON for the HTTP request body. The lookup keys from the main stream are in RowData format
     * and need to be serialized to JSON to construct the HTTP request. This is necessary to
     * properly handle complex Flink data types like arrays (ArrayData), nested structures
     * (RowData), and timestamps (TimestampData) which require schema-aware serialization.
     *
     * @param formatIdentifier the format identifier (e.g., "json")
     * @param readableConfig the configuration
     * @param dynamicTableFactoryContext the table factory context
     * @param lookupRow the lookup row containing schema information
     * @return the serialization schema
     */
    private SerializationSchema<RowData> createSerializationSchema(
            String formatIdentifier,
            ReadableConfig readableConfig,
            DynamicTableFactory.Context dynamicTableFactoryContext,
            LookupRow lookupRow) {
        final SerializationFormatFactory jsonFormatFactory =
                FactoryUtil.discoverFactory(
                        Thread.currentThread().getContextClassLoader(),
                        SerializationFormatFactory.class,
                        formatIdentifier);
        QueryFormatAwareConfiguration queryFormatAwareConfiguration =
                new QueryFormatAwareConfiguration(
                        LOOKUP_REQUEST_FORMAT.key() + "." + formatIdentifier,
                        (Configuration) readableConfig);
        EncodingFormat<SerializationSchema<RowData>> encoder =
                jsonFormatFactory.createEncodingFormat(
                        dynamicTableFactoryContext, queryFormatAwareConfiguration);

        // Get the DataType - prefer from lookupRow, fallback to context's physical row data type
        DataType dataType = lookupRow.getLookupPhysicalRowDataType();
        if (dataType == null) {
            // Fallback for tests or when not set - use the full table's physical row data type
            dataType =
                    dynamicTableFactoryContext
                            .getCatalogTable()
                            .getResolvedSchema()
                            .toPhysicalRowDataType();
        }

        if (readableConfig.get(ASYNC_POLLING)) {
            return new SynchronizedSerializationSchema<>(
                    encoder.createRuntimeEncoder(null, dataType));
        } else {
            return encoder.createRuntimeEncoder(null, dataType);
        }
    }
}
