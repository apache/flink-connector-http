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

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.http.LookupQueryCreator;
import org.apache.flink.connector.http.LookupQueryCreatorFactory;
import org.apache.flink.connector.http.clients.PollingClientFactory;
import org.apache.flink.connector.http.preprocessor.HeaderPreprocessor;
import org.apache.flink.connector.http.table.lookup.querycreators.GenericGetQueryCreatorFactory;
import org.apache.flink.connector.http.table.lookup.querycreators.GenericJsonQueryCreatorFactory;
import org.apache.flink.connector.http.utils.HttpHeaderUtils;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.DataTypes.Field;
import org.apache.flink.table.connector.Projection;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsLimitPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsReadingMetadata;
import org.apache.flink.table.connector.source.lookup.AsyncLookupFunctionProvider;
import org.apache.flink.table.connector.source.lookup.LookupFunctionProvider;
import org.apache.flink.table.connector.source.lookup.PartialCachingAsyncLookupProvider;
import org.apache.flink.table.connector.source.lookup.PartialCachingLookupProvider;
import org.apache.flink.table.connector.source.lookup.cache.LookupCache;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.functions.AsyncLookupFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.RowType.RowField;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;

import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.connector.http.table.lookup.HttpLookupConnectorOptions.LOOKUP_QUERY_CREATOR_IDENTIFIER;
import static org.apache.flink.connector.http.table.lookup.HttpLookupTableSourceFactory.row;

/** http lookyp table source. */
@Slf4j
public class HttpLookupTableSource
        implements LookupTableSource,
                SupportsReadingMetadata,
                SupportsProjectionPushDown,
                SupportsLimitPushDown {

    private DataType physicalRowDataType;

    private final HttpLookupConfig lookupConfig;

    private final DynamicTableFactory.Context dynamicTableFactoryContext;

    private final DecodingFormat<DeserializationSchema<RowData>> decodingFormat;
    @Nullable private final LookupCache cache;

    // --------------------------------------------------------------------------------------------
    // Mutable attributes
    // --------------------------------------------------------------------------------------------

    /** Data type that describes the final output of the source. */
    protected DataType producedDataType;

    /** Metadata that is appended at the end of a physical source row. */
    protected List<String> metadataKeys;

    public HttpLookupTableSource(
            DataType physicalRowDataType,
            HttpLookupConfig lookupConfig,
            DecodingFormat<DeserializationSchema<RowData>> decodingFormat,
            DynamicTableFactory.Context dynamicTablecontext,
            @Nullable LookupCache cache) {
        this.physicalRowDataType = physicalRowDataType;
        this.lookupConfig = lookupConfig;
        this.decodingFormat = decodingFormat;
        this.dynamicTableFactoryContext = dynamicTablecontext;
        this.cache = cache;
    }

    @Override
    public void applyProjection(int[][] projectedFields, DataType producedDataType) {
        physicalRowDataType = Projection.of(projectedFields).project(physicalRowDataType);
    }

    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext lookupContext) {
        log.debug("getLookupRuntimeProvider Entry");

        LookupRow lookupRow = extractLookupRow(lookupContext.getKeys());

        DeserializationSchema<RowData> responseSchemaDecoder =
                decodingFormat.createRuntimeDecoder(lookupContext, physicalRowDataType);

        LookupQueryCreatorFactory lookupQueryCreatorFactory =
                FactoryUtil.discoverFactory(
                        this.dynamicTableFactoryContext.getClassLoader(),
                        LookupQueryCreatorFactory.class,
                        lookupConfig
                                .getReadableConfig()
                                .getOptional(LOOKUP_QUERY_CREATOR_IDENTIFIER)
                                .orElse(
                                        (lookupConfig.getLookupMethod().equalsIgnoreCase("GET")
                                                ? GenericGetQueryCreatorFactory.IDENTIFIER
                                                : GenericJsonQueryCreatorFactory.IDENTIFIER)));
        ReadableConfig readableConfig = lookupConfig.getReadableConfig();
        LookupQueryCreator lookupQueryCreator =
                lookupQueryCreatorFactory.createLookupQueryCreator(
                        readableConfig, lookupRow, dynamicTableFactoryContext);

        PollingClientFactory pollingClientFactory =
                createPollingClientFactory(lookupQueryCreator, lookupConfig);

        return getLookupRuntimeProvider(lookupRow, responseSchemaDecoder, pollingClientFactory);
    }

    protected LookupRuntimeProvider getLookupRuntimeProvider(
            LookupRow lookupRow,
            DeserializationSchema<RowData> responseSchemaDecoder,
            PollingClientFactory pollingClientFactory) {
        MetadataConverter[] metadataConverters = {};
        if (this.metadataKeys != null) {
            metadataConverters = createMetadataConverters(this.metadataKeys);
        }

        HttpTableLookupFunction dataLookupFunction =
                new HttpTableLookupFunction(
                        pollingClientFactory,
                        responseSchemaDecoder,
                        lookupRow,
                        lookupConfig,
                        metadataConverters,
                        this.producedDataType);
        if (lookupConfig.isUseAsync()) {
            AsyncLookupFunction asyncLookupFunction =
                    new AsyncHttpTableLookupFunction(dataLookupFunction);
            if (cache != null) {
                log.info("Using async version of HttpLookupTable with cache.");
                return PartialCachingAsyncLookupProvider.of(asyncLookupFunction, cache);
            } else {
                log.info("Using async version of HttpLookupTable without cache.");
                return AsyncLookupFunctionProvider.of(asyncLookupFunction);
            }
        } else {
            if (cache != null) {
                log.info("Using blocking version of HttpLookupTable with cache.");
                return PartialCachingLookupProvider.of(dataLookupFunction, cache);
            } else {
                log.info("Using blocking version of HttpLookupTable without cache.");
                return LookupFunctionProvider.of(dataLookupFunction);
            }
        }
    }

    @Override
    public DynamicTableSource copy() {
        return new HttpLookupTableSource(
                physicalRowDataType,
                lookupConfig,
                decodingFormat,
                dynamicTableFactoryContext,
                cache);
    }

    @Override
    public String asSummaryString() {
        return "Http Lookup Table Source";
    }

    @Override
    public void applyLimit(long limit) {}

    @Override
    public boolean supportsNestedProjection() {
        return true;
    }

    private ReadableMetadata findReadableMetadataByKey(String key) {
        return Stream.of(HttpLookupTableSource.ReadableMetadata.values())
                .filter(rm -> rm.key.equals(key))
                .findFirst()
                .orElseThrow(IllegalStateException::new);
    }

    private MetadataConverter[] createMetadataConverters(List<String> metadataKeys) {
        return metadataKeys.stream()
                .map(this::findReadableMetadataByKey)
                .map(m -> m.converter)
                .toArray(MetadataConverter[]::new);
    }

    private PollingClientFactory createPollingClientFactory(
            LookupQueryCreator lookupQueryCreator, HttpLookupConfig lookupConfig) {

        HeaderPreprocessor headerPreprocessor =
                HttpHeaderUtils.createHeaderPreprocessor(lookupConfig.getReadableConfig());
        String lookupMethod = lookupConfig.getLookupMethod();

        HttpRequestFactory requestFactory =
                (lookupMethod.equalsIgnoreCase("GET"))
                        ? new GetRequestFactory(
                                lookupQueryCreator, headerPreprocessor, lookupConfig)
                        : new BodyBasedRequestFactory(
                                lookupMethod, lookupQueryCreator, headerPreprocessor, lookupConfig);
        log.info("requestFactory is " + requestFactory);
        return new JavaNetHttpPollingClientFactory(requestFactory);
    }

    private LookupRow extractLookupRow(int[][] keys) {

        LookupRow lookupRow = new LookupRow();

        List<String> fieldNames =
                TableSourceHelper.getFieldNames(physicalRowDataType.getLogicalType());

        List<LogicalType> fieldTypes =
                LogicalTypeChecks.getFieldTypes(physicalRowDataType.getLogicalType());

        List<Field> lookupDataTypes = new ArrayList<>();
        List<DataType> physicalRowDataTypes = physicalRowDataType.getChildren();

        int i = 0;
        for (int[] key : keys) {
            for (int keyIndex : key) {
                LogicalType type = fieldTypes.get(keyIndex);
                String name = fieldNames.get(keyIndex);
                lookupDataTypes.add(DataTypes.FIELD(name, physicalRowDataTypes.get(keyIndex)));
                lookupRow.addLookupEntry(extractKeyColumn(name, i++, type));
            }
        }

        lookupRow.setLookupPhysicalRowDataType(row(lookupDataTypes));
        return lookupRow;
    }

    private LookupSchemaEntry<RowData> extractKeyColumn(
            String name, int parentIndex, LogicalType type) {

        if (type instanceof RowType) {
            RowTypeLookupSchemaEntry rowLookupEntry =
                    new RowTypeLookupSchemaEntry(
                            name, RowData.createFieldGetter(type, parentIndex));
            List<RowField> fields = ((RowType) type).getFields();
            int index = 0;
            for (RowField rowField : fields) {
                rowLookupEntry.addLookupEntry(processRow(rowField, index++));
            }
            return rowLookupEntry;
        } else {
            return new RowDataSingleValueLookupSchemaEntry(
                    name, RowData.createFieldGetter(type, parentIndex));
        }
    }

    private LookupSchemaEntry<RowData> processRow(RowField rowField, int parentIndex) {
        LogicalType type1 = rowField.getType();
        String name = rowField.getName();
        if (type1 instanceof RowType) {
            RowTypeLookupSchemaEntry rowLookupEntry =
                    new RowTypeLookupSchemaEntry(
                            name, RowData.createFieldGetter(type1, parentIndex));
            int index = 0;
            List<RowField> rowFields = ((RowType) type1).getFields();
            for (RowField rowField1 : rowFields) {
                rowLookupEntry.addLookupEntry(processRow(rowField1, index++));
            }
            return rowLookupEntry;
        } else {
            return new RowDataSingleValueLookupSchemaEntry(
                    name, RowData.createFieldGetter(type1, parentIndex));
        }
    }

    @Override
    public Map<String, DataType> listReadableMetadata() {
        final Map<String, DataType> metadataMap = new LinkedHashMap<>();

        decodingFormat.listReadableMetadata().forEach((key, value) -> metadataMap.put(key, value));

        // according to convention, the order of the final row must be
        // PHYSICAL + FORMAT METADATA + CONNECTOR METADATA
        // where the format metadata has highest precedence
        // add connector metadata
        Stream.of(ReadableMetadata.values())
                .forEachOrdered(m -> metadataMap.putIfAbsent(m.key, m.dataType));
        return metadataMap;
    }

    @Override
    public void applyReadableMetadata(List<String> metadataKeys, DataType producedDataType) {
        // separate connector and format metadata
        final List<String> connectorMetadataKeys = new ArrayList<>(metadataKeys);
        final Map<String, DataType> formatMetadata = decodingFormat.listReadableMetadata();
        // store non connector keys and remove them from the connectorMetadataKeys.
        List<String> formatMetadataKeys = new ArrayList<>();
        Set<String> metadataKeysSet = metadataKeys.stream().collect(Collectors.toSet());
        for (ReadableMetadata rm : ReadableMetadata.values()) {
            String metadataKeyToCheck = rm.name();
            if (!metadataKeysSet.contains(metadataKeyToCheck)) {
                formatMetadataKeys.add(metadataKeyToCheck);
                connectorMetadataKeys.remove(metadataKeyToCheck);
            }
        }
        // push down format metadata keys
        if (formatMetadata.size() > 0) {
            final List<String> requestedFormatMetadataKeys =
                    formatMetadataKeys.stream().collect(Collectors.toList());
            decodingFormat.applyReadableMetadata(requestedFormatMetadataKeys);
        }
        this.metadataKeys = connectorMetadataKeys;
        this.producedDataType = producedDataType;
    }

    // --------------------------------------------------------------------------------------------
    // Metadata handling
    // --------------------------------------------------------------------------------------------
    enum ReadableMetadata {
        ERROR_STRING(
                "error-string", DataTypes.STRING(), new MetadataConverter.ErrorStringConverter()),
        HTTP_STATUS_CODE(
                "http-status-code",
                DataTypes.INT(),
                new MetadataConverter.HttpStatusCodeConverter()),
        HTTP_HEADERS(
                "http-headers",
                DataTypes.MAP(DataTypes.STRING(), DataTypes.ARRAY(DataTypes.STRING())),
                new MetadataConverter.HttpHeadersConverter()),
        HTTP_COMPLETION_STATE(
                "http-completion-state",
                DataTypes.STRING(),
                new MetadataConverter.HttpCompletionStateConverter());

        final String key;
        final DataType dataType;
        final MetadataConverter converter;

        ReadableMetadata(String key, DataType dataType, MetadataConverter converter) {
            this.key = key;
            this.dataType = dataType;
            this.converter = converter;
        }
    }
}
