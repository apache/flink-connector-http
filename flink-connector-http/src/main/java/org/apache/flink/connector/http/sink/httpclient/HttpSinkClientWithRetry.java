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

package org.apache.flink.connector.http.sink.httpclient;

import org.apache.flink.connector.http.clients.SinkHttpClientResponses;
import org.apache.flink.connector.http.sink.HttpSinkRequestEntry;

import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

/** Applies Resilience4j retry semantics to HTTP sink request attempts. */
class HttpSinkClientWithRetry {

    private final RetryConfig retryConfig;
    private final ScheduledExecutorService retryScheduler;

    HttpSinkClientWithRetry(RetryConfig retryConfig) {
        this.retryConfig = retryConfig;
        this.retryScheduler =
                Executors.newSingleThreadScheduledExecutor(new HttpSinkRetryThreadFactory());
    }

    CompletableFuture<SinkHttpClientResponses> send(
            List<HttpSinkRequestEntry> requestEntries,
            Function<List<HttpSinkRequestEntry>, CompletionStage<HttpSinkAttemptResult>>
                    attemptSubmitter) {
        var requestEntriesToSubmit = new AtomicReference<>(requestEntries);
        var responseAccumulator = new ResponseAccumulator();
        Retry retry =
                Retry.of(
                        "http-sink-connector",
                        RetryConfig.<HttpSinkAttemptResult>from(retryConfig)
                                .retryOnException(HttpSinkClientWithRetry::isRetryableIoException)
                                .retryOnResult(
                                        attemptResult -> {
                                            responseAccumulator.add(attemptResult);
                                            if (attemptResult.hasRetryableRequests()) {
                                                requestEntriesToSubmit.set(
                                                        attemptResult.getRetryableRequests());
                                                return true;
                                            }
                                            return false;
                                        })
                                .build());

        return Retry.<HttpSinkAttemptResult>decorateCompletionStage(
                        retry,
                        retryScheduler,
                        () -> attemptSubmitter.apply(requestEntriesToSubmit.get()))
                .get()
                .thenApply(
                        attemptResult -> {
                            if (attemptResult.hasRetryableRequests()) {
                                responseAccumulator.markRetriesExhausted(attemptResult);
                            }
                            return responseAccumulator.toResponse();
                        })
                .toCompletableFuture();
    }

    void close() {
        retryScheduler.shutdownNow();
    }

    static boolean isRetryableIoException(Throwable throwable) {
        Throwable current = throwable;
        while (current != null) {
            if (current instanceof IOException) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    private static final class ResponseAccumulator {
        private final List<HttpSinkRequestEntry> successfulRequests = new ArrayList<>();
        private final List<HttpSinkRequestEntry> retriableFailedRequests = new ArrayList<>();
        private final List<HttpSinkRequestEntry> fatalFailedRequests = new ArrayList<>();
        private final List<HttpSinkRequestEntry> ignoredRequests = new ArrayList<>();

        private void add(HttpSinkAttemptResult attemptResult) {
            successfulRequests.addAll(attemptResult.getSuccessfulRequests());
            fatalFailedRequests.addAll(attemptResult.getFatalFailedRequests());
            ignoredRequests.addAll(attemptResult.getIgnoredRequests());
        }

        private void markRetriesExhausted(HttpSinkAttemptResult attemptResult) {
            retriableFailedRequests.addAll(attemptResult.getRetryableRequests());
        }

        private SinkHttpClientResponses toResponse() {
            return new SinkHttpClientResponses(
                    successfulRequests,
                    retriableFailedRequests,
                    fatalFailedRequests,
                    ignoredRequests);
        }
    }

    private static final class HttpSinkRetryThreadFactory implements ThreadFactory {

        @Override
        public Thread newThread(Runnable runnable) {
            Thread thread = new Thread(runnable, "http-sink-retry-scheduler");
            thread.setDaemon(true);
            return thread;
        }
    }
}
