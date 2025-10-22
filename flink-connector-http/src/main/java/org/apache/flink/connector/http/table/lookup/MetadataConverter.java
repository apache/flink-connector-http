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

import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.StringData;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The metadata converters have a read method that is passed a HttpRowDataWrapper. The
 * implementations pick out the appropriate value of the metadata from this object.
 */
interface MetadataConverter extends Serializable {
    /**
     * @param httpRowDataWrapper an object that contains all metadata content
     * @return the metadata value for this MetadataConverter.
     */
    Object read(HttpRowDataWrapper httpRowDataWrapper);

    // Add these static inner classes inside the MetadataConverter interface
    class ErrorStringConverter implements MetadataConverter {
        private static final long serialVersionUID = 1L;

        @Override
        public Object read(HttpRowDataWrapper httpRowDataWrapper) {
            if (httpRowDataWrapper == null) {
                return null;
            }
            return StringData.fromString(httpRowDataWrapper.getErrorMessage());
        }
    }

    class HttpStatusCodeConverter implements MetadataConverter {
        private static final long serialVersionUID = 1L;

        @Override
        public Object read(HttpRowDataWrapper httpRowDataWrapper) {
            return (httpRowDataWrapper != null) ? httpRowDataWrapper.getHttpStatusCode() : null;
        }
    }

    class HttpHeadersConverter implements MetadataConverter {
        private static final long serialVersionUID = 1L;

        @Override
        public Object read(HttpRowDataWrapper httpRowDataWrapper) {
            if (httpRowDataWrapper == null) {
                return null;
            }
            Map<String, List<String>> httpHeadersMap = httpRowDataWrapper.getHttpHeadersMap();
            if (httpHeadersMap == null) {
                return null;
            }
            Map<StringData, ArrayData> stringDataMap = new HashMap<>();
            for (String key : httpHeadersMap.keySet()) {
                List<StringData> strDataList = new ArrayList<>();
                httpHeadersMap.get(key).stream()
                        .forEach((c) -> strDataList.add(StringData.fromString(c)));
                stringDataMap.put(
                        StringData.fromString(key), new GenericArrayData(strDataList.toArray()));
            }
            return new GenericMapData(stringDataMap);
        }
    }

    class HttpCompletionStateConverter implements MetadataConverter {
        private static final long serialVersionUID = 1L;

        @Override
        public Object read(HttpRowDataWrapper httpRowDataWrapper) {
            if (httpRowDataWrapper == null) {
                return null;
            }
            return StringData.fromString(httpRowDataWrapper.getHttpCompletionState().name());
        }
    }
}
