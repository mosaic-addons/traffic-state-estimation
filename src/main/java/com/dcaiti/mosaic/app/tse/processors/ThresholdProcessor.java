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

import com.dcaiti.mosaic.app.fcd.config.CTrafficStateEstimationServerApp;
import com.dcaiti.mosaic.app.fcd.data.FcdRecord;
import com.dcaiti.mosaic.app.fcd.messages.FcdUpdate;
import com.dcaiti.mosaic.app.tse.data.DatabaseAccess;
import com.dcaiti.mosaic.app.tse.persistence.FcdDataStorage;
import com.dcaiti.mosaic.app.tse.persistence.FcdDatabaseHelper;
import com.dcaiti.mosaic.app.tse.persistence.ScenarioDatabaseHelper;
import org.eclipse.mosaic.lib.database.Database;
import org.eclipse.mosaic.lib.util.gson.TimeFieldAdapter;
import org.eclipse.mosaic.rti.TIME;

import com.google.common.math.Quantiles;
import com.google.gson.annotations.JsonAdapter;
import org.apache.commons.math3.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * This class is used to compute the thresholds needed to compute the relative traffic metric.
 *
 * @see SpatioTemporalProcessor
 * @see SpatioTemporalTrafficMetric
 * @see FcdDatabaseHelper
 */
public class ThresholdProcessor extends TimeBasedProcessor<FcdRecord, FcdUpdate> implements DatabaseAccess {

    private final static String IDENTIFIER = createIdentifier(ThresholdProcessor.class);
    public static final int MIN_RECORDED_TRAVERSALS_FOR_TL_HEURISTIC = 10;
    public static final int MAX_RECORDED_TRAVERSAL_FOR_TL_HEURISTIC = 400;
    // === CONFIG ===
    /**
     * As an estimation of the RedPhase, this value will be applied to connections with
     * a significant difference in travel times close to the estimated duration.
     */
    @JsonAdapter(TimeFieldAdapter.NanoSeconds.class)
    public long defaultRedLightDuration = 45 * TIME.SECOND;
    /**
     * Thresholds will only be computed and used for RTSM, if the number of samples in the past is larger than this.
     */
    public int minTraversalsForThreshold = 10;
    /**
     * If true, the relative traffic metrics will be recomputed every time the thresholds are recomputed.
     * This may take long, as there can be a lot of traversals.
     */
    public boolean recomputeAllRtsmWithNewThresholds = false;
    // === CONFIG ===
    /**
     * Mosaic scenario database, holding data regarding the road network.
     */
    private Database networkDatabase;
    /**
     * Gives access to the FcdDatabase.
     */
    private FcdDataStorage fcdDataStorage;
    /**
     * Holds estimated red light durations.
     */
    private final Map<String, Long> redLightDurations = new HashMap<>();

    @Override
    public String getIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public void withDataStorage(Database networkDatabase, FcdDataStorage fcdDataStorage) {
        this.networkDatabase = networkDatabase;
        this.fcdDataStorage = fcdDataStorage;
    }

    @Override
    public void triggerEvent(long eventTime) {
        long start = System.currentTimeMillis();
        computeThresholds(eventTime);
        long duration = (System.currentTimeMillis() - start);
        logThresholdComputation(eventTime, duration * TIME.MILLI_SECOND);
        if (recomputeAllRtsmWithNewThresholds) {
            calculateRtsm();
        }
    }

    /**
     * Computes thresholds using current data from DB and inserting them to thresholds table.
     * Uses Google Guava for percentiles.
     *
     * @param simulationTime Time at which the threshold computation started. Needed for further analysis of the resulting data.
     * @see <a href="https://github.com/google/guava">Google Guava on GitHub</a>
     * @see <a href="https://github.com/google/guava/blob/master/guava/src/com/google/common/math/Quantiles.java">Guava Quantiles</a>
     */
    private void computeThresholds(long simulationTime) {
        if (triggerInterval <= 0) {
            return;
        }
        Map<String, Double> temporalThresholds = computeTemporalThresholds();
        Map<String, Double> spatialThresholds = computeSpatialThresholds(temporalThresholds);
        // handle the possibility of no spatial Threshold, as percentile is above temporal threshold
        if (temporalThresholds.size() != spatialThresholds.size()) {
            ArrayList<String> notInBoth = new ArrayList<>();
            temporalThresholds.forEach((connection, temporalThreshold) -> {
                if (!spatialThresholds.containsKey(connection)) {
                    notInBoth.add(connection);
                }
            });
            spatialThresholds.forEach((connection, temporalThreshold) -> {
                if (!temporalThresholds.containsKey(connection)) {
                    notInBoth.add(connection);
                }
            });
            notInBoth.forEach(connection -> {
                spatialThresholds.remove(connection);
                temporalThresholds.remove(connection);
            });
        }
        fcdDataStorage.insertThresholds(temporalThresholds, spatialThresholds, simulationTime);
    }

    /**
     * Temporal threshold as done by Yoon et al. with 5% of 5th percentile as red light duration.
     * Does so only if more than {@link #minTraversalsForThreshold} traversals have been recorded for a connection.
     *
     * @return mapping of connectionID and current temporal threshold
     */
    private Map<String, Double> computeTemporalThresholds() {
        Map<String, List<Long>> traversalTimes = fcdDataStorage.getTraversalTimes();
        Map<String, Double> percentiles = computeTraversalTimePercentiles(traversalTimes, 5);
        updateHeuristicRedLightDurations(traversalTimes);

        Map<String, Double> thresholds = new HashMap<>();
        percentiles.forEach((connection, temporalPercentile) -> {
            double length = ScenarioDatabaseHelper.calcLengthByNodes(networkDatabase.getConnection(connection));
            double time = (temporalPercentile + redLightDurations.getOrDefault(connection, 0L)) / (double) TIME.SECOND;
            thresholds.putIfAbsent(connection, length / time);
        });
        return thresholds;
    }

    /**
     * Computes the given percentile for all traversal times and connections in the database
     * using google guavas' {@link Quantiles} framework.
     * Does so only if more than {@link #minTraversalsForThreshold} traversals have been recorded for a connection.
     *
     * @param traversalTimes  mapping of connectionID to a List with all traversal times
     * @param percentileIndex index of percentile to be computed
     * @return Mapping of connectionID to its current percentile
     * @see <a href="https://github.com/google/guava">Google Guava on GitHub</a>
     * @see <a href="https://github.com/google/guava/blob/master/guava/src/com/google/common/math/Quantiles.java">Guava Quantiles</a>
     */
    private Map<String, Double> computeTraversalTimePercentiles(Map<String, List<Long>> traversalTimes, int percentileIndex) {
        Map<String, Double> percentiles = new HashMap<>();

        traversalTimes.forEach((conn, times) -> {
            if (times.size() <= minTraversalsForThreshold) {
                return;
            }
            List<Long> filteredTimes = times.stream().filter(t -> t > 5).collect(Collectors.toCollection(ArrayList::new));
            if (!filteredTimes.isEmpty()) {
                percentiles.putIfAbsent(conn, Quantiles.percentiles().index(percentileIndex).compute(times));
            }
        });
        return percentiles;
    }

    /**
     * Updates the {@link #redLightDurations} map with a given redLightDuration from the {@link CTrafficStateEstimationServerApp},
     * if the difference between 5th and 60th percentile of travel time for a connection is above the given estimation
     * of red light duration, but not greater than 3 times the duration.
     *
     * @param traversalTimes Travel times of all connections.
     */
    private void updateHeuristicRedLightDurations(Map<String, List<Long>> traversalTimes) {
        for (Map.Entry<String, List<Long>> entry : traversalTimes.entrySet()) {
            if (entry.getValue().size() <= MIN_RECORDED_TRAVERSALS_FOR_TL_HEURISTIC
                    || entry.getValue().size() > MAX_RECORDED_TRAVERSAL_FOR_TL_HEURISTIC
                    || redLightDurations.containsKey(entry.getKey())
            ) {
                continue;
            }
            double percentileDiff = Math.abs(Quantiles.percentiles().index(60).compute(entry.getValue()) // 60th percentile
                    - Quantiles.percentiles().index(5).compute(entry.getValue())); // 5th percentile
            if (percentileDiff >= defaultRedLightDuration && percentileDiff <= 3 * defaultRedLightDuration) {
                redLightDurations.put(entry.getKey(), defaultRedLightDuration);
            }
        }
    }

    /**
     * computes spatial thresholds based on prior computed temporal thresholds as done by Yoon et al.
     * Does so only if more than {@link #minTraversalsForThreshold} traversals have been recorded for a connection.
     *
     * @param temporalThresholds temporal thresholds needed for spatial threshold
     * @return Map of connection Id and threshold
     */
    private Map<String, Double> computeSpatialThresholds(Map<String, Double> temporalThresholds) {
        Map<String, ArrayList<Pair<Double, Double>>> meanSpeedsMap = fcdDataStorage.getMeanSpeeds();

        Map<String, Double> percentiles = new HashMap<>();

        meanSpeedsMap.forEach((conn, meanSpeeds) -> {
            if (meanSpeeds.size() <= minTraversalsForThreshold) {
                return;
            }
            List<Double> filteredSpatialMeansSpeeds =
                    meanSpeeds.stream().filter(meanSpeed -> meanSpeed.getFirst() >= temporalThresholds.get(conn))
                            .map(Pair::getSecond)
                            .collect(Collectors.toList());

            if (!filteredSpatialMeansSpeeds.isEmpty()) {
                percentiles.putIfAbsent(conn, Quantiles.percentiles().index(5).compute(filteredSpatialMeansSpeeds));
            }
        });
        return percentiles;
    }

    private void logThresholdComputation(long time, long duration) {
        String logString = "Computing Thresholds at {} took {} [ interval: {} recomputeRTSM: {} ]";
        logger.info(logString, TIME.format(time), TIME.format(duration), TIME.format(triggerInterval), recomputeAllRtsmWithNewThresholds);
        LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME).info(
                logString, TIME.format(time), TIME.format(duration), TIME.format(triggerInterval), recomputeAllRtsmWithNewThresholds);
    }

    @Override
    public void shutdown(long shutdownTime) {
        if (triggerInterval > 0 && getPreviousTriggerTime() != shutdownTime) {
            triggerEvent(shutdownTime);
        }
        calculateRtsm();
        redLightDurations.clear();
    }

    private void calculateRtsm() {
        long start = System.currentTimeMillis();
        SpatioTemporalTrafficMetric.recomputeRelativeTrafficMetricsForAllTraversals(fcdDataStorage);
        long duration = (System.currentTimeMillis() - start) * TIME.MILLI_SECOND;
        logRtsmCalculation(duration);
    }

    private void logRtsmCalculation(long duration) {
        String logString = "Computing RTSM took {}";
        logger.info(logString, TIME.format(duration));
        LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME).info(logString, TIME.format(duration));
    }

    @Override
    public void handleUpdate(FcdUpdate update) {
        // nop
    }
}
