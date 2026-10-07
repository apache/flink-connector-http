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

package org.apache.flink.connector.http.sink.httpclient;

import org.apache.flink.connector.http.sink.HttpSinkRequestEntry;

import io.github.resilience4j.core.IntervalFunction;
import io.github.resilience4j.retry.RetryConfig;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link HttpSinkClientWithRetry}. */
class HttpSinkClientWithRetryTest {

    @Test
    public void testRetriesOnlyRetryableRequests() {
        HttpSinkRequestEntry successfulEntry = new HttpSinkRequestEntry("POST", new byte[] {1});
        HttpSinkRequestEntry retryableEntry = new HttpSinkRequestEntry("POST", new byte[] {2});
        AtomicInteger calls = new AtomicInteger();
        HttpSinkClientWithRetry retryingClient =
                new HttpSinkClientWithRetry(
                        RetryConfig.custom()
                                .maxAttempts(2)
                                .intervalFunction(IntervalFunction.of(Duration.ofMillis(1)))
                                .build());

        var response =
                retryingClient
                        .send(
                                List.of(successfulEntry, retryableEntry),
                                requestEntries -> {
                                    if (calls.getAndIncrement() == 0) {
                                        assertThat(requestEntries)
                                                .containsExactly(successfulEntry, retryableEntry);
                                        HttpSinkAttemptResult attemptResult =
                                                new HttpSinkAttemptResult();
                                        attemptResult.addSuccessfulRequests(
                                                List.of(successfulEntry));
                                        attemptResult.addRetryableRequests(List.of(retryableEntry));
                                        return CompletableFuture.completedFuture(attemptResult);
                                    }

                                    assertThat(requestEntries).containsExactly(retryableEntry);
                                    HttpSinkAttemptResult attemptResult =
                                            new HttpSinkAttemptResult();
                                    attemptResult.addSuccessfulRequests(List.of(retryableEntry));
                                    return CompletableFuture.completedFuture(attemptResult);
                                })
                        .join();

        assertThat(response.getSuccessfulRequests())
                .containsExactly(successfulEntry, retryableEntry);
        assertThat(response.getRetriableFailedRequests()).isEmpty();
        assertThat(response.getFatalFailedRequests()).isEmpty();
        assertThat(calls).hasValue(2);
    }

    @Test
    public void testAccumulatesIgnoredRequestsAcrossAttempts() {
        HttpSinkRequestEntry ignoredEntry = new HttpSinkRequestEntry("POST", new byte[] {1});
        HttpSinkRequestEntry retryableEntry = new HttpSinkRequestEntry("POST", new byte[] {2});
        AtomicInteger calls = new AtomicInteger();
        HttpSinkClientWithRetry retryingClient =
                new HttpSinkClientWithRetry(
                        RetryConfig.custom()
                                .maxAttempts(2)
                                .intervalFunction(IntervalFunction.of(Duration.ofMillis(1)))
                                .build());

        var response =
                retryingClient
                        .send(
                                List.of(ignoredEntry, retryableEntry),
                                requestEntries -> {
                                    HttpSinkAttemptResult attemptResult =
                                            new HttpSinkAttemptResult();
                                    if (calls.getAndIncrement() == 0) {
                                        attemptResult.addIgnoredRequests(List.of(ignoredEntry));
                                        attemptResult.addRetryableRequests(List.of(retryableEntry));
                                    } else {
                                        attemptResult.addSuccessfulRequests(
                                                List.of(retryableEntry));
                                    }
                                    return CompletableFuture.completedFuture(attemptResult);
                                })
                        .join();

        assertThat(response.getSuccessfulRequests()).containsExactly(retryableEntry);
        assertThat(response.getIgnoredRequests()).containsExactly(ignoredEntry);
        assertThat(response.getRetriableFailedRequests()).isEmpty();
        assertThat(response.getFatalFailedRequests()).isEmpty();
    }

    @Test
    public void testReturnsRetryableRequestsAfterRetriesAreExhausted() {
        HttpSinkRequestEntry retryableEntry = new HttpSinkRequestEntry("POST", new byte[] {1});
        AtomicInteger calls = new AtomicInteger();
        HttpSinkClientWithRetry retryingClient =
                new HttpSinkClientWithRetry(
                        RetryConfig.custom()
                                .maxAttempts(2)
                                .intervalFunction(IntervalFunction.of(Duration.ofMillis(1)))
                                .build());

        var response =
                retryingClient
                        .send(
                                List.of(retryableEntry),
                                requestEntries -> {
                                    calls.incrementAndGet();
                                    assertThat(requestEntries).containsExactly(retryableEntry);
                                    HttpSinkAttemptResult attemptResult =
                                            new HttpSinkAttemptResult();
                                    attemptResult.addRetryableRequests(List.of(retryableEntry));
                                    return CompletableFuture.completedFuture(attemptResult);
                                })
                        .join();

        assertThat(response.getSuccessfulRequests()).isEmpty();
        assertThat(response.getRetriableFailedRequests()).containsExactly(retryableEntry);
        assertThat(response.getFatalFailedRequests()).isEmpty();
        assertThat(calls).hasValue(2);
    }

    @Test
    public void testRetriesIoExceptionFromFailedCompletionStage() {
        HttpSinkRequestEntry retryableEntry = new HttpSinkRequestEntry("POST", new byte[] {1});
        AtomicInteger calls = new AtomicInteger();
        HttpSinkClientWithRetry retryingClient =
                new HttpSinkClientWithRetry(
                        RetryConfig.custom()
                                .maxAttempts(2)
                                .intervalFunction(IntervalFunction.of(Duration.ofMillis(1)))
                                .build());

        var response =
                retryingClient
                        .send(
                                List.of(retryableEntry),
                                requestEntries -> {
                                    assertThat(requestEntries).containsExactly(retryableEntry);
                                    if (calls.getAndIncrement() == 0) {
                                        return CompletableFuture.failedFuture(
                                                new java.io.IOException("connection reset"));
                                    }
                                    HttpSinkAttemptResult attemptResult =
                                            new HttpSinkAttemptResult();
                                    attemptResult.addSuccessfulRequests(List.of(retryableEntry));
                                    return CompletableFuture.completedFuture(attemptResult);
                                })
                        .join();

        assertThat(response.getSuccessfulRequests()).containsExactly(retryableEntry);
        assertThat(response.getRetriableFailedRequests()).isEmpty();
        assertThat(response.getFatalFailedRequests()).isEmpty();
        assertThat(calls).hasValue(2);
    }

    @Test
    public void testDoesNotRetryNonIoExceptionFromFailedCompletionStage() {
        HttpSinkRequestEntry retryableEntry = new HttpSinkRequestEntry("POST", new byte[] {1});
        AtomicInteger calls = new AtomicInteger();
        HttpSinkClientWithRetry retryingClient =
                new HttpSinkClientWithRetry(
                        RetryConfig.custom()
                                .maxAttempts(2)
                                .intervalFunction(IntervalFunction.of(Duration.ofMillis(1)))
                                .build());

        assertThatThrownBy(
                        () ->
                                retryingClient
                                        .send(
                                                List.of(retryableEntry),
                                                requestEntries -> {
                                                    assertThat(requestEntries)
                                                            .containsExactly(retryableEntry);
                                                    calls.incrementAndGet();
                                                    return CompletableFuture.failedFuture(
                                                            new RuntimeException(
                                                                    "callback failed"));
                                                })
                                        .join())
                .hasRootCauseInstanceOf(RuntimeException.class)
                .hasRootCauseMessage("callback failed");

        assertThat(calls).hasValue(1);
    }
}
