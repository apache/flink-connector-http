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

package org.apache.flink.connector.http.sink;

import org.apache.flink.metrics.HistogramStatistics;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link HttpSinkRequestLatencyHistogram}. */
class HttpSinkRequestLatencyHistogramTest {

    @Test
    public void testStatisticsForLatencySamples() {
        HttpSinkRequestLatencyHistogram histogram = new HttpSinkRequestLatencyHistogram(10);

        histogram.update(10);
        histogram.update(20);
        histogram.update(30);

        HistogramStatistics statistics = histogram.getStatistics();

        assertThat(histogram.getCount()).isEqualTo(3);
        assertThat(statistics.size()).isEqualTo(3);
        assertThat(statistics.getValues()).containsExactly(10, 20, 30);
        assertThat(statistics.getMin()).isEqualTo(10);
        assertThat(statistics.getMax()).isEqualTo(30);
        assertThat(statistics.getMean()).isEqualTo(20);
        assertThat(statistics.getQuantile(0.5)).isEqualTo(20);
    }

    @Test
    public void testBoundsRetainedLatencySamples() {
        HttpSinkRequestLatencyHistogram histogram = new HttpSinkRequestLatencyHistogram(2);

        histogram.update(10);
        histogram.update(20);
        histogram.update(30);

        HistogramStatistics statistics = histogram.getStatistics();

        assertThat(histogram.getCount()).isEqualTo(3);
        assertThat(statistics.size()).isEqualTo(2);
        assertThat(statistics.getValues()).containsExactly(20, 30);
        assertThat(statistics.getMin()).isEqualTo(20);
        assertThat(statistics.getMax()).isEqualTo(30);
    }

    @Test
    public void testEmptyStatistics() {
        HttpSinkRequestLatencyHistogram histogram = new HttpSinkRequestLatencyHistogram(2);
        HistogramStatistics statistics = histogram.getStatistics();

        assertThat(histogram.getCount()).isZero();
        assertThat(statistics.size()).isZero();
        assertThat(statistics.getValues()).isEmpty();
        assertThat(statistics.getMin()).isZero();
        assertThat(statistics.getMax()).isZero();
        assertThat(statistics.getMean()).isZero();
        assertThat(statistics.getStdDev()).isZero();
        assertThat(statistics.getQuantile(0.95)).isZero();
    }

    @Test
    public void testRejectsInvalidSampleSize() {
        assertThatThrownBy(() -> new HttpSinkRequestLatencyHistogram(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("sampleSize must be greater than 0.");
    }

    @Test
    public void testConcurrentUpdates() throws InterruptedException {
        HttpSinkRequestLatencyHistogram histogram = new HttpSinkRequestLatencyHistogram(128);
        int threadCount = 4;
        int updatesPerThread = 1000;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threadCount);

        try {
            for (int thread = 0; thread < threadCount; thread++) {
                executor.submit(
                        () -> {
                            try {
                                start.await();
                                for (int update = 0; update < updatesPerThread; update++) {
                                    histogram.update(update + 1);
                                }
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            } finally {
                                done.countDown();
                            }
                        });
            }

            start.countDown();

            assertThat(done.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(histogram.getCount()).isEqualTo((long) threadCount * updatesPerThread);
            assertThat(histogram.getStatistics().size()).isEqualTo(128);
            for (long value : histogram.getStatistics().getValues()) {
                assertThat(value).isPositive();
            }
        } finally {
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        }
    }
}
