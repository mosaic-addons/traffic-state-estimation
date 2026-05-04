/*
 * Copyright (c) 2021 Fraunhofer FOKUS and others. All rights reserved.
 *
 * See the NOTICE file(s) distributed with this work for additional
 * information regarding copyright ownership.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License 2.0 which is available at
 * http://www.eclipse.org/legal/epl-2.0
 *
 * SPDX-License-Identifier: EPL-2.0
 *
 * Contact: mosaic@fokus.fraunhofer.de
 */

package com.dcaiti.mosaic.app.tse.processors;

import com.dcaiti.mosaic.app.fxd.data.FcdTraversal;
import org.eclipse.mosaic.fed.application.ambassador.util.UnitLogger;
import org.eclipse.mosaic.lib.math.Aggregator;
import org.eclipse.mosaic.lib.util.gson.TimeFieldAdapter;
import org.eclipse.mosaic.rti.TIME;

import com.google.gson.annotations.JsonAdapter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Processor that aggregates spatio-temporal metrics for connections over configurable time intervals.
 * Unlike {@link SpatioTemporalProcessor}, which writes metrics for each individual traversal,
 * this processor collects metrics in memory and writes aggregated averages at interval boundaries.
 * <p>
 * Configuration parameters:
 * <ul>
 *     <li>{@link #aggregationInterval}: Time interval for aggregation (default: 15 minutes)</li>
 *     <li>{@link #processingDelay}: Delay before flushing an interval to allow late-arriving data (default: 5 minutes)</li>
 *     <li>{@link #spatialMeanSpeedChunkSize}: Chunk size for spatial mean speed calculation (default: 15m)</li>
 * </ul>
 */
public class AggregatedSpatioTemporalProcessor extends AbstractSpatioTemporalProcessor {

    /**
     * Time interval over which to aggregate metrics [ns].
     * Default: 15 minutes.
     */
    @JsonAdapter(TimeFieldAdapter.NanoSeconds.class)
    public long aggregationInterval = 15 * TIME.MINUTE;

    /**
     * Processing delay: time to wait before flushing an interval, allowing late-arriving data [ns].
     * Default: 5 minutes.
     */
    @JsonAdapter(TimeFieldAdapter.NanoSeconds.class)
    public long processingDelay = 5 * TIME.MINUTE;

    /**
     * Aggregation buffer: Map<ConnectionId, Map<IntervalStart, IntervalMetrics>>.
     * Each IntervalMetrics holds aggregators for temporal and spatial mean speeds for a specific interval.
     */
    private final Map<String, Map<Long, IntervalMetrics>> aggregationBuffer = new HashMap<>();

    /**
     * Tracks the highest timestamp seen so far to determine which intervals can be flushed.
     */
    private long maxSeenTimestamp = 0;

    /**
     * Helper class to accumulate temporal and spatial mean speed statistics for a single interval.
     * Uses MOSAIC's {@link Aggregator} for statistical calculations.
     */
    private static class IntervalMetrics {
        private final Aggregator temporalMeanSpeed = new Aggregator();
        private final Aggregator spatialMeanSpeed = new Aggregator();
        private final Aggregator naiveMeanSpeed = new Aggregator();
        private final Aggregator speedPerformanceIndex = new Aggregator();

        void add(double temporalMeanSpeed, double spatialMeanSpeed, double naiveMeanSpeed, double speedPerformanceIndex) {
            this.temporalMeanSpeed.add(temporalMeanSpeed);
            this.spatialMeanSpeed.add(spatialMeanSpeed);
            this.naiveMeanSpeed.add(naiveMeanSpeed);
            this.speedPerformanceIndex.add(speedPerformanceIndex);
        }

        Aggregator getTemporalMeanSpeed() {
            return temporalMeanSpeed;
        }

        Aggregator getSpatialMeanSpeed() {
            return spatialMeanSpeed;
        }
        
        Aggregator getNaiveMeanSpeed() {
            return naiveMeanSpeed;
        }
        
        Aggregator getSpeedPerformanceIndex() {
            return speedPerformanceIndex;
        }
    }

    @Override
    public void initialize(UnitLogger logger) {
        this.logger = logger;
        logger.info("AggregatedSpatioTemporalProcessor initialized with interval={}ns, delay={}ns",
                aggregationInterval, processingDelay);
    }

    @Override
    public void shutdown(long shutdownTime) {
        // Flush all remaining intervals at shutdown
        logger.info("Flushing all remaining aggregated metrics at shutdown (time={})", shutdownTime);
        flushAllIntervals();
    }

    @Override
    public void onConnectionTraversal(String unitId, FcdTraversal traversal) {
        // Compute metrics for this traversal using base class logic
        ComputedMetrics metrics = computeTraversalMetrics(unitId, traversal);
        if (metrics == null) {
            return; // Invalid or incomplete traversal
        }

        // Determine which interval this traversal belongs to
        long timestamp = metrics.timestamp();
        long intervalStart = (timestamp / aggregationInterval) * aggregationInterval;

        // Update max seen timestamp
        if (timestamp > maxSeenTimestamp) {
            maxSeenTimestamp = timestamp;
            // Flush completed intervals (those older than maxSeenTimestamp - processingDelay)
            flushCompletedIntervals();
        }

        // Add metrics to the appropriate interval aggregator
        String connectionId = traversal.getConnectionId();
        aggregationBuffer
                .computeIfAbsent(connectionId, k -> new HashMap<>())
                .computeIfAbsent(intervalStart, k -> new IntervalMetrics())
                .add(metrics.temporalMeanSpeed(), metrics.spatialMeanSpeed(), metrics.naiveMeanSpeed(), metrics.speedPerformanceIndex());
    }

    /**
     * Flushes all intervals that are older than (maxSeenTimestamp - processingDelay).
     */
    private void flushCompletedIntervals() {
        long flushThreshold = maxSeenTimestamp - processingDelay;
        List<String> connectionsToRemove = new ArrayList<>();

        for (Map.Entry<String, Map<Long, IntervalMetrics>> connEntry : aggregationBuffer.entrySet()) {
            String connectionId = connEntry.getKey();
            Map<Long, IntervalMetrics> intervals = connEntry.getValue();

            List<Long> intervalsToFlush = new ArrayList<>();
            for (Map.Entry<Long, IntervalMetrics> intervalEntry : intervals.entrySet()) {
                long intervalStart = intervalEntry.getKey();
                long intervalEnd = intervalStart + aggregationInterval;

                // Flush if interval end is before the flush threshold
                if (intervalEnd <= flushThreshold) {
                    intervalsToFlush.add(intervalStart);
                }
            }

            // Flush and remove completed intervals
            for (Long intervalStart : intervalsToFlush) {
                IntervalMetrics metrics = intervals.get(intervalStart);
                long intervalEnd = intervalStart + aggregationInterval;
                fcdDataStorage.insertAggregatedTraversalMetrics(
                        connectionId,
                        intervalStart,
                        intervalEnd,
                        metrics.getTemporalMeanSpeed().average(),
                        metrics.getSpatialMeanSpeed().average(),
                        metrics.getNaiveMeanSpeed().average(),
                        metrics.getSpeedPerformanceIndex().average(),
                        metrics.getTemporalMeanSpeed().total()
                );
                intervals.remove(intervalStart);
                logger.debug("Flushed aggregated metrics for connection {} at interval [{}, {}] with {} samples",
                        connectionId, intervalStart, intervalEnd, metrics.getTemporalMeanSpeed().total());
            }

            // Mark connection for removal if no intervals remain
            if (intervals.isEmpty()) {
                connectionsToRemove.add(connectionId);
            }
        }

        // Clean up empty connection entries
        for (String connectionId : connectionsToRemove) {
            aggregationBuffer.remove(connectionId);
        }
    }

    /**
     * Flushes all remaining intervals (called at shutdown).
     */
    private void flushAllIntervals() {
        for (Map.Entry<String, Map<Long, IntervalMetrics>> connEntry : aggregationBuffer.entrySet()) {
            String connectionId = connEntry.getKey();
            Map<Long, IntervalMetrics> intervals = connEntry.getValue();

            for (Map.Entry<Long, IntervalMetrics> intervalEntry : intervals.entrySet()) {
                long intervalStart = intervalEntry.getKey();
                long intervalEnd = intervalStart + aggregationInterval;
                IntervalMetrics metrics = intervalEntry.getValue();

                fcdDataStorage.insertAggregatedTraversalMetrics(
                        connectionId,
                        intervalStart,
                        intervalEnd,
                        metrics.getTemporalMeanSpeed().average(),
                        metrics.getSpatialMeanSpeed().average(),
                        metrics.getNaiveMeanSpeed().average(),
                        metrics.getSpeedPerformanceIndex().average(),
                        metrics.getTemporalMeanSpeed().total()
                );
                logger.debug("Flushed aggregated metrics for connection {} at interval [{}, {}] with {} samples (shutdown)",
                        connectionId, intervalStart, intervalEnd, metrics.getTemporalMeanSpeed().total());
            }
        }
        aggregationBuffer.clear();
    }


}
