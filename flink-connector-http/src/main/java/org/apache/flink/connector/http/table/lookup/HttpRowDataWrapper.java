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

import org.apache.flink.table.data.RowData;

import lombok.Builder;
import lombok.Data;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * This bean contains the RowData information (the response body as a flink RowData). It also
 * contains information from the http response, namely the http headers map and the http status code
 * where available. The extra information is for the metadata columns.
 */
@Builder
@Data
public class HttpRowDataWrapper {
    private final Collection<RowData> data;
    private final String errorMessage;
    private final Map<String, List<String>> httpHeadersMap;
    private final Integer httpStatusCode;
    private final HttpCompletionState httpCompletionState;

    public boolean shouldIgnore() {
        return (this.data != null
                && this.data.isEmpty()
                && this.errorMessage == null
                && this.httpHeadersMap == null
                && this.httpStatusCode == null
                && httpCompletionState == HttpCompletionState.SUCCESS);
    }
}
