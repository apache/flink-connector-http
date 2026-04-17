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

import com.github.tomakehurst.wiremock.extension.ResponseTransformerV2;
import com.github.tomakehurst.wiremock.http.Response;
import com.github.tomakehurst.wiremock.stubbing.ServeEvent;
import com.github.tomakehurst.wiremock.verification.LoggedRequest;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Abstract base class for WireMock response transformers that generate JSON responses with
 * counter-based IDs and configurable UUID behavior.
 *
 * <p>Subclasses must provide:
 *
 * <ul>
 *   <li>Transformer name via {@link #getName()}
 *   <li>JSON template via {@link #getJsonTemplate()}
 *   <li>UUID generation strategy via {@link #getUuidValue()}
 * </ul>
 */
public abstract class AbstractJsonTransform implements ResponseTransformerV2 {

    private final AtomicInteger counter = new AtomicInteger(0);

    private static final String BASE_JSON_TEMPLATE =
            "{\n"
                    + "\t\"id\": \"&COUNTER&\",\n"
                    + "\t\"id2\": \"&COUNTER_2&\",\n"
                    + "\t\"uuid\": \"&UUID&\",\n"
                    + "\t\"picture\": \"http://placehold.it/32x32\",\n"
                    + "\t\"msg\": \"&PARAM&, cnt: &COUNTER&\",\n"
                    + "\t\"age\": 30,\n"
                    + "\t\"eyeColor\": \"green\",\n"
                    + "\t\"name\": \"Marva Fischer\",\n"
                    + "\t\"gender\": \"female\",\n"
                    + "\t\"company\": \"SILODYNE\",\n"
                    + "\t\"email\": \"marvafischer@silodyne.com\",\n"
                    + "\t\"phone\": \"+1 (990) 562-2120\",\n"
                    + "\t\"address\": \"601 Auburn Place, Bynum, New York, 7057\",\n"
                    + "\t\"about\": \"Proident Lorem et duis nisi tempor elit occaecat laboris"
                    + " dolore magna Lorem consequat. Deserunt velit minim nisi consectetur duis "
                    + "amet labore cupidatat. Pariatur sunt occaecat qui reprehenderit ipsum ex culpa "
                    + "ullamco ex duis adipisicing commodo sunt. Ad cupidatat magna ad in officia "
                    + "irure aute duis culpa et. Magna esse adipisicing consequat occaecat. Excepteur amet "
                    + "dolore occaecat sit officia dolore elit in cupidatat non anim.\\r\\n\",\n"
                    + "\t\"registered\": \"2020-07-11T11:13:32 -02:00\",\n"
                    + "\t\"latitude\": -35.237843,\n"
                    + "\t\"longitude\": 60.386104,\n"
                    + "\t\"tags\": [\n"
                    + "\t\t\"officia\",\n"
                    + "\t\t\"eiusmod\",\n"
                    + "\t\t\"labore\",\n"
                    + "\t\t\"ex\",\n"
                    + "\t\t\"aliqua\",\n"
                    + "\t\t\"consectetur\",\n"
                    + "\t\t\"excepteur\"\n"
                    + "\t],\n"
                    + "\t\"friends\": [\n"
                    + "\t\t{\n"
                    + "\t\t\t\"id\": 0,\n"
                    + "\t\t\t\"name\": \"Kemp Newman\"\n"
                    + "\t\t},\n"
                    + "\t\t{\n"
                    + "\t\t\t\"id\": 1,\n"
                    + "\t\t\t\"name\": \"Sears Blackburn\"\n"
                    + "\t\t},\n"
                    + "\t\t{\n"
                    + "\t\t\t\"id\": 2,\n"
                    + "\t\t\t\"name\": \"Lula Rogers\"\n"
                    + "\t\t}\n"
                    + "\t],\n"
                    + "\t\"&NESTED_OBJECT&\": {\n"
                    + "\t\t\"isActive\": true,\n"
                    + "\t\t\"nestedDetails\": {\n"
                    + "\t\t\t\"index\": 0,\n"
                    + "\t\t\t\"guid\": \"d81fc542-6b49-4d59-8fb9-d57430d4871d\",\n"
                    + "\t\t\t\"balance\": \"$1,729.34\"\n"
                    + "\t\t}\n"
                    + "\t},\n"
                    + "\t\"greeting\": \"Hello, Marva Fischer! You have 7 unread messages.\",\n"
                    + "\t\"favoriteFruit\": \"banana\"\n"
                    + "}";

    @Override
    public final Response transform(Response response, ServeEvent serveEvent) {
        int cnt = counter.getAndIncrement();
        LoggedRequest request = serveEvent.getRequest();
        return Response.response()
                .body(generateResponse(request.getUrl(), cnt))
                .status(response.getStatus())
                .statusMessage(response.getStatusMessage())
                .build();
    }

    /**
     * Returns the JSON template with placeholders for dynamic values.
     *
     * <p>Subclasses that need a different JSON structure should override this method. By default,
     * returns the base template with &NESTED_OBJECT& placeholder.
     *
     * <p>Supported placeholders:
     *
     * <ul>
     *   <li>&PARAM& - Request URL
     *   <li>&COUNTER& - Current counter value
     *   <li>&COUNTER_2& - Counter value + 1
     *   <li>&UUID& - UUID value (if used)
     *   <li>&NESTED_OBJECT& - Nested object name (if used)
     * </ul>
     *
     * @return JSON template string
     */
    protected String getJsonTemplate() {
        return BASE_JSON_TEMPLATE;
    }

    /**
     * Returns the name for the nested object in the JSON response.
     *
     * <p>Default implementation returns null (no nested object placeholder replacement). Override
     * this method in subclasses that use the &NESTED_OBJECT& placeholder.
     *
     * @return nested object name, or null if not used
     */
    protected String getNestedObjectName() {
        return null;
    }

    /**
     * Returns the UUID value to use in responses.
     *
     * @return fixed UUID string for deterministic behavior
     */
    private String getUuidValue() {
        return "fbb68a46-80a9-46da-9d40-314b5287079c";
    }

    private String generateResponse(String param, int counter) {
        String response =
                getJsonTemplate()
                        .replace("&PARAM&", param)
                        .replace("&COUNTER&", String.valueOf(counter))
                        .replace("&COUNTER_2&", String.valueOf(counter + 1));

        String uuid = getUuidValue();
        if (uuid != null) {
            response = response.replace("&UUID&", uuid);
        }

        String nestedObjectName = getNestedObjectName();
        if (nestedObjectName != null) {
            response = response.replace("&NESTED_OBJECT&", nestedObjectName);
        }

        return response;
    }

    @Override
    public boolean applyGlobally() {
        return false;
    }
}
