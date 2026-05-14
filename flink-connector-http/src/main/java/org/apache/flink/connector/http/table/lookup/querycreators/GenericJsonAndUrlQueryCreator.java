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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.http.LookupArg;
import org.apache.flink.connector.http.LookupQueryCreator;
import org.apache.flink.connector.http.table.lookup.LookupQueryInfo;
import org.apache.flink.connector.http.table.lookup.LookupRow;
import org.apache.flink.connector.http.utils.SerializationSchemaUtils;
import org.apache.flink.table.api.DataTypes.Field;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.FieldsDataType;
import org.apache.flink.types.Row;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Generic JSON and URL query creator; in addition to be able to map columns to json requests, it
 * allows url inserts to be mapped to column names using templating. <br>
 * <br>
 * For PUT and POST, parameters are mapped to the json body e.g. for the body template "id1;id2" and
 * url of http://base. At lookup time with values of id1=1 and id2=2 as call of http/base will be
 * issued with a json payload of {"id1":1,"id2":2} <br>
 *
 * For all http methods, url segments and query parameters can be used to include lookup up values.
 * Using the map from <code>GenericJsonAndUrlQueryCreator.REQUEST_URL_MAP</code> which has a key of the
 * insert name and the value of the associated column. e.g. for <code>GenericJsonAndUrlQueryCreator.REQUEST_URL_MAP
 * </code> = "key1":"col1" and url of http://base/{key1}. At lookup time with values of col1="aaaa"
 * a call of http/base/aaaa will be issued. For query parameters, the query param should be
 * supplied in the URL with a place-holder that will be resolved using <code>
 * GenericJsonAndUrlQueryCreator.REQUEST_URL_MAP</code>
 */
@Slf4j
public class GenericJsonAndUrlQueryCreator implements LookupQueryCreator {
    private static final long serialVersionUID = 1L;
    private static final Pattern TEMPLATE_PLACEHOLDER_PATTERN =
            Pattern.compile("\\{\\{([^}]+)\\}\\}");

    // not final so we can mutate for unit test
    private SerializationSchema<RowData> serializationSchema;
    private boolean schemaOpened = false;
    private LookupRow lookupRow;
    private final String httpMethod;
    private final Map<String, String> requestUrlMap;
    private final String bodyTemplate;

    /**
     * Construct a Generic JSON and URL query creator.
     *
     * @param httpMethod the requested http method
     * @param serializationSchema serialization schema for RowData
     * @param requestUrlMap url map
     * @param bodyTemplate template string for request body with placeholders like {@code
     *     {{fieldName}}}
     * @param lookupRow lookup row itself.
     */
    public GenericJsonAndUrlQueryCreator(
            final String httpMethod,
            final SerializationSchema<RowData> serializationSchema,
            final Map<String, String> requestUrlMap,
            final String bodyTemplate,
            final LookupRow lookupRow) {
        this.httpMethod = httpMethod;
        this.serializationSchema = serializationSchema;
        this.lookupRow = lookupRow;
        this.requestUrlMap = requestUrlMap;
        this.bodyTemplate = bodyTemplate;
    }

    @VisibleForTesting
    void setSerializationSchema(SerializationSchema<RowData> serializationSchema) {
        this.serializationSchema = serializationSchema;
    }

    @Override
    public LookupQueryInfo createLookupQuery(final RowData lookupDataRow) {
        this.checkOpened();

        final String lookupQuery;
        final Collection<LookupArg> lookupArgs = lookupRow.convertToLookupArgs(lookupDataRow);
        ObjectNode jsonObject;
        try {
            jsonObject =
                    (ObjectNode)
                            ObjectMapperAdapter.instance()
                                    .readTree(serializationSchema.serialize(lookupDataRow));
        } catch (IOException e) {
            String message = "Unable to parse the lookup arguments to json.";
            log.error(message, e);
            throw new RuntimeException(message, e);
        }

        if (httpMethod.equalsIgnoreCase("GET")) {
            // For GET requests, query parameters are now handled via URL placeholders
            // No query string is generated here
            lookupQuery = "";
        } else {
            // Body-based queries (POST/PUT)
            // Check if body template is provided
            if (bodyTemplate != null && !bodyTemplate.trim().isEmpty()) {
                // Use template substitution
                lookupQuery = substituteTemplate(bodyTemplate, jsonObject);
            } else {
                // No template provided - no body (consistent with ElasticSearchLiteQueryCreator)
                lookupQuery = "";
            }
        }
        // add the path map
        final Map<String, String> pathBasedUrlParams =
                createPathBasedParams(lookupArgs, requestUrlMap);

        return new LookupQueryInfo(lookupQuery, null, pathBasedUrlParams);
    }

    /**
     * Substitutes placeholders in the template with values from the JSON object. Placeholders are
     * in the format {@code {{fieldName}}} where fieldName is a top-level field in the JSON object.
     *
     * @param template the template string with placeholders
     * @param jsonObject the JSON object containing field values
     * @return the template with placeholders replaced by actual values
     */
    private String substituteTemplate(String template, ObjectNode jsonObject) {
        Matcher matcher = TEMPLATE_PLACEHOLDER_PATTERN.matcher(template);

        StringBuilder result = new StringBuilder();
        while (matcher.find()) {
            String fieldName = matcher.group(1);
            JsonNode fieldValue = jsonObject.get(fieldName);

            if (fieldValue == null) {
                throw new IllegalArgumentException(
                        String.format(
                                "Template placeholder {{%s}} references a field that does not exist in the lookup row",
                                fieldName));
            }

            String valueStr =
                    fieldValue.isTextual()
                            ? "\"" + fieldValue.asText() + "\""
                            : fieldValue.toString();

            matcher.appendReplacement(result, java.util.regex.Matcher.quoteReplacement(valueStr));
        }
        matcher.appendTail(result);
        return result.toString();
    }

    /**
     * Create a Row from a RowData and DataType.
     *
     * @param lookupRowData the lookup RowData
     * @param rowType the datatype
     * @return row return row
     */
    @VisibleForTesting
    static Row rowDataToRow(final RowData lookupRowData, final DataType rowType) {
        Preconditions.checkNotNull(lookupRowData);
        Preconditions.checkNotNull(rowType);

        final Row row = Row.withNames();
        final List<Field> rowFields = FieldsDataType.getFields(rowType);

        for (int idx = 0; idx < rowFields.size(); idx++) {
            final String fieldName = rowFields.get(idx).getName();
            final Object fieldValue = ((GenericRowData) lookupRowData).getField(idx);
            row.setField(fieldName, fieldValue);
        }
        return row;
    }

    /**
     * Create map of the json key to the lookup argument value. This is used for body based content.
     *
     * @param args lookup arguments
     * @param objectNode object node
     * @return map of field content to the lookup argument value.
     */
    private Map<String, String> createBodyBasedParams(
            final Collection<LookupArg> args, ObjectNode objectNode) {
        Map<String, String> mapOfJsonKeyToLookupArg = new LinkedHashMap<>();
        Iterator<Map.Entry<String, JsonNode>> iterator = objectNode.fields();
        iterator.forEachRemaining(
                field -> {
                    for (final LookupArg arg : args) {
                        if (arg.getArgName().equals(field.getKey())) {
                            String keyForMap = field.getKey();
                            mapOfJsonKeyToLookupArg.put(keyForMap, arg.getArgValue());
                        }
                    }
                });

        return mapOfJsonKeyToLookupArg;
    }

    /**
     * Create map of the json key to the lookup argument value. This is used for path based content.
     *
     * @param args lookup arguments
     * @param urlMap map of insert name to column name
     * @return map of field content to the lookup argument value.
     */
    private Map<String, String> createPathBasedParams(
            final Collection<LookupArg> args, Map<String, String> urlMap) {
        Map<String, String> mapOfJsonKeyToLookupArg = new LinkedHashMap<>();
        if (urlMap != null) {
            for (String key : urlMap.keySet()) {
                for (final LookupArg arg : args) {
                    if (arg.getArgName().equals(key)) {
                        mapOfJsonKeyToLookupArg.put(urlMap.get(key), arg.getArgValue());
                    }
                }
            }
        }
        return mapOfJsonKeyToLookupArg;
    }

    private void checkOpened() {
        if (!this.schemaOpened) {
            try {
                this.serializationSchema.open(
                        SerializationSchemaUtils.createSerializationInitContext(
                                GenericJsonAndUrlQueryCreator.class));
                this.schemaOpened = true;
            } catch (final Exception e) {
                final String message =
                        "Failed to initialize serialization schema for "
                                + GenericJsonAndUrlQueryCreator.class;
                log.error(message, e);
                throw new FlinkRuntimeException(message, e);
            }
        }
    }
}
