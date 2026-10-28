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

package org.apache.flink.connector.http.sink;

import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.HistogramStatistics;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.LongAdder;

/** Bounded histogram for HTTP sink request latency samples. */
class HttpSinkRequestLatencyHistogram implements Histogram {

    private final AtomicLongArray samples;

    private final AtomicInteger nextIndex;

    private final LongAdder count;

    HttpSinkRequestLatencyHistogram(int sampleSize) {
        if (sampleSize <= 0) {
            throw new IllegalArgumentException("sampleSize must be greater than 0.");
        }
        this.samples = new AtomicLongArray(sampleSize);
        this.nextIndex = new AtomicInteger();
        this.count = new LongAdder();
    }

    @Override
    public void update(long value) {
        int index =
                nextIndex.getAndUpdate(
                        current -> current == samples.length() - 1 ? 0 : current + 1);
        samples.set(index, value);
        count.increment();
    }

    @Override
    public long getCount() {
        return count.sum();
    }

    @Override
    public HistogramStatistics getStatistics() {
        int size = (int) Math.min(count.sum(), samples.length());
        long[] values = new long[size];
        for (int i = 0; i < size; i++) {
            values[i] = samples.get(i);
        }
        Arrays.sort(values);
        return new LatencyHistogramStatistics(values);
    }

    private static class LatencyHistogramStatistics extends HistogramStatistics {

        private final long[] values;

        private LatencyHistogramStatistics(long[] values) {
            this.values = values;
        }

        @Override
        public double getQuantile(double quantile) {
            if (values.length == 0) {
                return 0;
            }

            double normalizedQuantile = Math.max(0, Math.min(quantile, 1));
            int index = (int) Math.ceil(normalizedQuantile * values.length) - 1;
            return values[Math.max(0, index)];
        }

        @Override
        public long[] getValues() {
            return Arrays.copyOf(values, values.length);
        }

        @Override
        public int size() {
            return values.length;
        }

        @Override
        public double getMean() {
            if (values.length == 0) {
                return 0;
            }

            long sum = 0;
            for (long value : values) {
                sum += value;
            }
            return (double) sum / values.length;
        }

        @Override
        public double getStdDev() {
            if (values.length == 0) {
                return 0;
            }

            double mean = getMean();
            double variance = 0;
            for (long value : values) {
                double difference = value - mean;
                variance += difference * difference;
            }
            return Math.sqrt(variance / values.length);
        }

        @Override
        public long getMax() {
            if (values.length == 0) {
                return 0;
            }
            return values[values.length - 1];
        }

        @Override
        public long getMin() {
            if (values.length == 0) {
                return 0;
            }
            return values[0];
        }
    }
}
