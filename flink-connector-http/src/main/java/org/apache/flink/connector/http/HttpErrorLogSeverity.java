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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.http;

/** Defines the SLF4J severity level for HTTP error logging. */
public enum HttpErrorLogSeverity {
    /** No error-level logging (DEBUG only, current behavior). */
    OFF,
    /** Log errors at INFO level. */
    INFO,
    /** Log errors at WARN level. */
    WARN,
    /** Log errors at ERROR level (default for production). */
    ERROR;

    public static HttpErrorLogSeverity fromString(String level) {
        if (level == null) {
            return ERROR;
        }
        try {
            return HttpErrorLogSeverity.valueOf(level.toUpperCase());
        } catch (IllegalArgumentException e) {
            return ERROR;
        }
    }
}
