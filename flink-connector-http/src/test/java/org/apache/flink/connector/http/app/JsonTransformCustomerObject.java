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

package org.apache.flink.connector.http.app;

/**
 * Wiremock Extension that prepares HTTP REST endpoint response body. This extension is stateful,
 * every next response will have values like id == counter, id2 == counter + 1 and uuid =
 * randomValue value in its response, where counter is incremented for every subsequent request.
 * This mock has a customer object as the nested object name, so a unit test can supply customer in
 * the request and have the same name field with a different type in the response.
 */
public class JsonTransformCustomerObject extends AbstractJsonTransform {

    public static final String NAME = "JsonTransformCustomerObject";

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    protected String getNestedObjectName() {
        return "customer";
    }
}
