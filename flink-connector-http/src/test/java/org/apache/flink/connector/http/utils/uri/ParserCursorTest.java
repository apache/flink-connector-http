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

package org.apache.flink.connector.http.utils.uri;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link ParserCursor}. */
class ParserCursorTest {

    @Test
    public void testBoundsValidation() {
        assertThatThrownBy(() -> new ParserCursor(-1, 1))
                .isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> new ParserCursor(1, -1))
                .isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    public void testUpdatePostValidation() {
        ParserCursor cursor = new ParserCursor(1, 2);

        assertThatThrownBy(() -> cursor.updatePos(0)).isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> cursor.updatePos(3)).isInstanceOf(IndexOutOfBoundsException.class);
    }
}
