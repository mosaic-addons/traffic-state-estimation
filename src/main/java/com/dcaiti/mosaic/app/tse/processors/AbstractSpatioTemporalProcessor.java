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

import com.dcaiti.mosaic.app.fxd.data.FcdRecord;
import com.dcaiti.mosaic.app.fxd.data.FcdTraversal;
import com.dcaiti.mosaic.app.tse.data.DatabaseAccess;
import com.dcaiti.mosaic.app.tse.persistence.FcdDataStorage;
import com.dcaiti.mosaic.app.tse.persistence.ScenarioDatabaseHelper;
import org.eclipse.mosaic.fed.application.ambassador.util.UnitLogger;
import org.eclipse.mosaic.lib.database.Database;
import org.eclipse.mosaic.lib.util.gson.UnitFieldAdapter;
import org.eclipse.mosaic.rti.TIME;

import com.google.gson.annotations.JsonAdapter;
import org.apache.commons.math3.analysis.interpolation.LinearInterpolator;
import org.apache.commons.math3.analysis.polynomials.PolynomialSplineFunction;
import org.apache.commons.math3.exception.OutOfRangeException;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * Abstract base class for spatio-temporal processors that compute temporal and spatial mean speeds
 * from FCD traversals.
 * <p>
 * This class provides common functionality for:
 * <ul>
 *     <li>Building traversal lists with previous/following records</li>
 *     <li>Interpolating time-distance and speed-distance functions</li>
 *     <li>Computing temporal and spatial mean speeds</li>
 *     <li>Calculating traversal lengths with threshold handling</li>
 * </ul>
 * <p>
 * Subclasses must implement {@link TraversalBasedProcessor#onConnectionTraversal} to define
 * how computed metrics are used (e.g., direct storage, aggregation, etc.).
 */
public abstract class AbstractSpatioTemporalProcessor implements TraversalBasedProcessor<FcdRecord, FcdTraversal>, DatabaseAccess {
    protected static final int CONNECTION_LENGTH_THRESHOLD = 5;

    /**
     * Each connection will be dissected into parts of this length and
     * the spatial mean speed will be averaged over measurements on these points. [m]
     */
    @JsonAdapter(UnitFieldAdapter.DistanceMeters.class)
    public double spatialMeanSpeedChunkSize = 15;

    protected UnitLogger logger;
    protected Database networkDatabase;
    protected FcdDataStorage fcdDataStorage;

    /**
     * Helper record to hold computed metrics for a traversal.
     */
    protected record ComputedMetrics(
            long timestamp,
            double temporalMeanSpeed,
            double spatialMeanSpeed,
            double traversalTime
    ) {}

    @Override
    public void withDataStorage(Database networkDatabase, FcdDataStorage fcdDataStorage) {
        this.networkDatabase = networkDatabase;
        this.fcdDataStorage = fcdDataStorage;
    }

    /**
     * Computes spatio-temporal metrics for a given traversal.
     * Returns null if the traversal is invalid or incomplete.
     *
     * @param vehicleId vehicle identifier
     * @param traversal traversal object containing FCD records
     * @return ComputedMetrics containing temporal/spatial speeds and traversal time, or null if invalid
     */
    protected ComputedMetrics computeTraversalMetrics(String vehicleId, FcdTraversal traversal) {
        final LinkedList<FcdRecord> records = buildTraversalList(traversal);
        String connectionId = traversal.getConnectionId();

        if (records.size() < 3) {
            logger.error("Invalid traversal during computation of sub metrics for vehicle {} on connection {}", vehicleId, connectionId);
            return null;
        }

        // Check if traversal is complete
        FcdRecord previousRecord = records.peekFirst();
        FcdRecord firstRecordOnConnection = records.get(1);
        FcdRecord followingRecord = records.peekLast();
        if (previousRecord == null || followingRecord == null || firstRecordOnConnection == null
                || previousRecord.getConnectionId().equals(firstRecordOnConnection.getConnectionId())
                || firstRecordOnConnection.getConnectionId().equals(followingRecord.getConnectionId())
        ) {
            logger.debug("Incomplete traversal during computation of sub metrics for vehicle {} on connection {}", vehicleId, connectionId);
            return null;
        }

        // Calculate splines for distance-time and distance-speed relations
        double[] distanceOffsets = new double[records.size()];
        double[] speeds = new double[records.size()];
        double[] timeStamps = new double[records.size()];

        timeStamps[0] = (double) records.get(0).getTimeStamp();
        speeds[0] = records.get(0).getSpeed();
        distanceOffsets[0] = records.get(0).getOffset();

        for (int i = 1; i < records.size(); i++) {
            timeStamps[i] = (double) records.get(i).getTimeStamp();
            speeds[i] = records.get(i).getSpeed();
            distanceOffsets[i] = Math.max(records.get(i).getOffset(), distanceOffsets[i - 1] + 0.001);
        }

        PolynomialSplineFunction distanceTimeSpline = interpolateTimeDistanceFunction(distanceOffsets, timeStamps);
        PolynomialSplineFunction distanceSpeedSpline = interpolateSpeedDistanceFunction(distanceOffsets, speeds);

        double length = calculateTraversalLength(connectionId, distanceTimeSpline);

        try {
            double traversalTime = distanceTimeSpline.value(length) - distanceTimeSpline.value(0);
            double temporalMeanSpeed = (length / traversalTime) * TIME.SECOND;
            double spatialMeanSpeed = computeSpatialMeanSpeed(records, distanceSpeedSpline);

            long timestamp = traversal.getTraversal().get(traversal.getTraversal().size() - 1).getTimeStamp();

            return new ComputedMetrics(timestamp, temporalMeanSpeed, spatialMeanSpeed, traversalTime);
        } catch (OutOfRangeException e) {
            logger.debug("Error during computeTraversalMetrics() for {} on Connection {}: {}", vehicleId, connectionId, e.getMessage());
            return null;
        }
    }

    /**
     * Build a list of records for spline generation, including previous and following records with adjusted offsets.
     * <p>
     * Note: Not exactly sure if we need the previous and following record, but it's part of the original algorithm.
     *
     * @param traversal the traversal object
     * @return a {@link LinkedList} of {@link FcdRecord} representing a traversal
     */
    protected LinkedList<FcdRecord> buildTraversalList(FcdTraversal traversal) {
        final LinkedList<FcdRecord> records = new LinkedList<>();
        if (traversal.getPreviousRecord() != null) {
            double offsetPreviousRecord = -traversal.getPreviousRecord().getPosition()
                    .distanceTo(networkDatabase.getConnection(traversal.getConnectionId()).getNodes().get(0).getPosition());
            records.add(new FcdRecord.Builder(traversal.getPreviousRecord()).withOffset(offsetPreviousRecord).build());
        }
        records.addAll(traversal.getTraversal());
        if (traversal.getFollowingRecord() != null && !records.isEmpty()) {
            FcdRecord lastTraversalRecord = records.peekLast();
            double offsetFollowingRecord = lastTraversalRecord.getOffset() + lastTraversalRecord.getPosition()
                    .distanceTo(traversal.getFollowingRecord().getPosition());
            records.add(new FcdRecord.Builder(traversal.getFollowingRecord()).withOffset(offsetFollowingRecord).build());
        }
        return records;
    }

    /**
     * Calculates the effective length of a traversal, using either the connection's nominal length
     * or the last knot from the spline if they're within {@link #CONNECTION_LENGTH_THRESHOLD}.
     *
     * @param connectionId connection identifier
     * @param distanceTimeSpline interpolated time-distance function
     * @return effective traversal length
     */
    protected double calculateTraversalLength(String connectionId, PolynomialSplineFunction distanceTimeSpline) {
        double length = ScenarioDatabaseHelper.calcLengthByNodes(networkDatabase.getConnection(connectionId));

        int lastKnotIndex = distanceTimeSpline.getKnots().length - 1;
        // if the calculated length is less than 5m longer, then just use last offset as length
        if (length > distanceTimeSpline.getKnots()[lastKnotIndex]
                && length - CONNECTION_LENGTH_THRESHOLD <= distanceTimeSpline.getKnots()[lastKnotIndex]) {
            length = distanceTimeSpline.getKnots()[lastKnotIndex];
        }
        return length;
    }

    /**
     * Computes spatial-mean-speed by interpolating the speed over distance and averaging over equidistant points
     * with a distance of {@link #spatialMeanSpeedChunkSize}.
     *
     * @param records list of records of the connection-traversal
     * @param distanceSpeedSpline interpolated speed-distance function
     * @return the spatial mean speed
     */
    protected double computeSpatialMeanSpeed(LinkedList<FcdRecord> records, PolynomialSplineFunction distanceSpeedSpline) {
        double cur = Math.ceil(records.getFirst().getOffset());
        double end = Math.floor(records.getLast().getOffset());

        // if connection is shorter than interval, just compute average
        if (end - cur < spatialMeanSpeedChunkSize) {
            return records.stream().mapToDouble(FcdRecord::getSpeed).sum() / records.size();
        }
        final List<Double> speeds = new ArrayList<>();
        while (end - cur >= spatialMeanSpeedChunkSize) {
            speeds.add(distanceSpeedSpline.value(cur));
            cur += spatialMeanSpeedChunkSize;
        }
        return speeds.stream().mapToDouble(Double::doubleValue).sum() / speeds.size();
    }

    /**
     * Interpolates a speed-distance function using the recorded data for a connection.
     * Uses linear interpolation between recorded speeds.
     *
     * @param distanceOffsets the distance offsets from the start of the connection
     * @param speeds the recorded speeds at each offset
     * @return {@link PolynomialSplineFunction} representing the speed-distance function
     */
    protected static PolynomialSplineFunction interpolateSpeedDistanceFunction(double[] distanceOffsets, double[] speeds) {
        return interpolateTimeDistanceFunction(distanceOffsets, speeds);
    }

    /**
     * Interpolates a time-distance function for the given records.
     * Uses linear interpolation between recorded timestamps.
     *
     * @param distanceOffsets the distance offsets from the start of the connection
     * @param timeStamps the timestamps at each offset
     * @return {@link PolynomialSplineFunction} representing the time-distance function
     */
    protected static PolynomialSplineFunction interpolateTimeDistanceFunction(double[] distanceOffsets, double[] timeStamps) {
        LinearInterpolator linearInterpolator = new LinearInterpolator();
        return linearInterpolator.interpolate(distanceOffsets, timeStamps);
    }

    /**
     * Computes the naive temporal-mean-speed by averaging speeds of the records on the connection.
     *
     * @param records list of records (may include previous/following records)
     * @return naive temporal mean speed
     */
    protected double computeNaiveTemporalMeanSpeed(LinkedList<FcdRecord> records) {
        double sum = 0;
        int count = 0;
        // Get connectionId from second record (first might be previous connection)
        String connectionId = records.get(1).getConnectionId();
        for (FcdRecord record : records) {
            if (record.getConnectionId().equals(connectionId)) {
                sum = sum + record.getSpeed();
                count++;
            }
        }
        return sum / count;
    }
}
