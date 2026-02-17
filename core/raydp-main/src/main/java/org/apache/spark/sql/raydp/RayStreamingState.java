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

package org.apache.spark.sql.raydp;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Thread-safe state container for the Ray → Spark streaming bridge.
 *
 * Python pushes resolved Arrow IPC data here via py4j forward calls.
 * The JVM-side MicroBatchStream reads from this state.
 *
 * Invariant: {@code setLatestBatchId(N)} is called only after
 * {@code addBatch(N, data)} has completed, guaranteeing data availability.
 */
public class RayStreamingState {

    // -- Static registry --
    private static final ConcurrentHashMap<String, RayStreamingState> REGISTRY =
            new ConcurrentHashMap<>();

    public static void register(String streamId, RayStreamingState state) {
        REGISTRY.put(streamId, state);
    }

    public static RayStreamingState get(String streamId) {
        return REGISTRY.get(streamId);
    }

    public static RayStreamingState remove(String streamId) {
        return REGISTRY.remove(streamId);
    }

    // -- Instance fields --
    private final String streamId;
    private final String schemaJson;
    private final String timeZoneId;

    private final ConcurrentHashMap<Integer, byte[]> batches = new ConcurrentHashMap<>();
    private volatile int latestBatchId = -1;
    private volatile int committedBatchId = -1;
    private volatile boolean complete = false;
    private volatile String errorMessage = null;

    private final Object monitor = new Object();

    public RayStreamingState(String streamId, String schemaJson, String timeZoneId) {
        this.streamId = streamId;
        this.schemaJson = schemaJson;
        this.timeZoneId = timeZoneId;
    }

    // -- Python API (called via py4j) --

    public void addBatch(int batchId, byte[] arrowIpcBytes) {
        batches.put(batchId, arrowIpcBytes);
    }

    public void setLatestBatchId(int batchId) {
        synchronized (monitor) {
            this.latestBatchId = batchId;
            monitor.notifyAll();
        }
    }

    public void setComplete() {
        synchronized (monitor) {
            this.complete = true;
            monitor.notifyAll();
        }
    }

    public void setError(String message) {
        synchronized (monitor) {
            this.errorMessage = message;
            this.complete = true;
            monitor.notifyAll();
        }
    }

    // -- JVM API (called by MicroBatchStream) --

    public String getStreamId() {
        return streamId;
    }

    public String getSchemaJson() {
        return schemaJson;
    }

    public String getTimeZoneId() {
        return timeZoneId;
    }

    /**
     * Returns the latest batch ID that has been fully pushed (data + id set).
     * Returns -1 if no batches available yet.
     */
    public int getLatestBatchId() {
        return latestBatchId;
    }

    /**
     * Blocking read of batch data. Waits until the batch is available or
     * the stream completes/errors.
     *
     * @param batchId   the batch to read
     * @param timeoutMs maximum wait time in milliseconds
     * @return Arrow IPC bytes, or null if timed out or stream errored
     */
    public byte[] getBatchData(int batchId, long timeoutMs) throws InterruptedException {
        byte[] data = batches.get(batchId);
        if (data != null) {
            return data;
        }

        synchronized (monitor) {
            long deadline = System.currentTimeMillis() + timeoutMs;
            while (true) {
                data = batches.get(batchId);
                if (data != null) {
                    return data;
                }
                if (errorMessage != null) {
                    throw new RuntimeException("Stream error: " + errorMessage);
                }
                if (complete && batchId > latestBatchId) {
                    return null;
                }
                long remaining = deadline - System.currentTimeMillis();
                if (remaining <= 0) {
                    return null;
                }
                monitor.wait(remaining);
            }
        }
    }

    /**
     * Commit (acknowledge) all batches up to (exclusive) the given batch ID.
     * Removes committed batch data to free memory.
     */
    public void commit(int upToBatchId) {
        int prev = this.committedBatchId;
        this.committedBatchId = upToBatchId;
        // Remove data for committed batches
        for (int i = prev + 1; i < upToBatchId; i++) {
            batches.remove(i);
        }
    }

    public int getCommittedBatchId() {
        return committedBatchId;
    }

    public boolean isComplete() {
        return complete;
    }

    public String getErrorMessage() {
        return errorMessage;
    }
}
