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

import com.dcaiti.mosaic.app.fcd.TrafficStateEstimationServerApp;
import com.dcaiti.mosaic.app.fcd.data.FcdRecord;
import com.dcaiti.mosaic.app.fcd.data.FcdTraversal;
import com.dcaiti.mosaic.app.tse.data.DatabaseAccess;
import com.dcaiti.mosaic.app.tse.persistence.FcdDataStorage;
import com.dcaiti.mosaic.app.tse.persistence.FcdDatabaseHelper;
import com.dcaiti.mosaic.app.tse.persistence.ScenarioDatabaseHelper;
import org.eclipse.mosaic.fed.application.ambassador.util.UnitLogger;
import org.eclipse.mosaic.lib.database.Database;
import org.eclipse.mosaic.lib.util.gson.UnitFieldAdapter;
import org.eclipse.mosaic.rti.TIME;

import com.google.common.collect.Iterables;
import com.google.gson.annotations.JsonAdapter;
import org.apache.commons.math3.analysis.interpolation.LinearInterpolator;
import org.apache.commons.math3.analysis.polynomials.PolynomialSplineFunction;
import org.apache.commons.math3.exception.OutOfRangeException;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * Processes new FCD data. Holds methods to compute base-metrics and handles the record-buffer-queue for each veh.
 * It mainly handles computing the spatial and temporal mean speeds, once a veh leaves a connection.
 * This Data needs to be preprocessed, as it is the base from which the thresholds are derived.
 *
 * @see FcdDatabaseHelper
 * @see TrafficStateEstimationServerApp
 */
public class SpatioTemporalProcessor implements TraversalBasedProcessor<FcdRecord, FcdTraversal>, DatabaseAccess {
    private final static int CONNECTION_LENGTH_THRESHOLD = 5;
    /**
     * Each connection will be dissected into parts of this length and
     * the spatial mean speed will be averaged over measurements on these points. [m]
     */
    @JsonAdapter(UnitFieldAdapter.DistanceMeters.class)
    public double spatialMeanSpeedChunkSize = 15;

    private UnitLogger logger;
    private Database networkDatabase;
    /**
     * needed to store and retrieve any data from the FcdDatabase.
     */
    private FcdDataStorage fcdDataStorage;

    /**
     * Polls the last connection from the buffer, performs preprocessing and stores resulting metrics in db.
     *
     * @param vehicleId to compute sub-metrics for
     * @param traversal {@link FcdTraversal} containing all records of a connection traversal plus previous and following record
     */
    private void computeSubMetrics(String vehicleId, FcdTraversal traversal) {
        final LinkedList<FcdRecord> records = buildTraversalList(traversal);
        String connectionId = traversal.getConnectionId();

        if (records.size() < 3) { //  we expect at least one record on the previous, one on the current, and one on the following connection
            logger.error("Invalid traversal during computation of sub metrics for vehicle {} on connection {}", vehicleId, connectionId);
            return;
        }
        // quit if incomplete traversal
        FcdRecord previousRecord = records.peekFirst();
        FcdRecord firstRecordOnConnection = records.get(1);
        FcdRecord followingRecord = records.peekLast();
        if (previousRecord == null || followingRecord == null || firstRecordOnConnection == null
                || previousRecord.getConnectionId().equals(firstRecordOnConnection.getConnectionId())
                || firstRecordOnConnection.getConnectionId().equals(followingRecord.getConnectionId())
        ) {
            // execution will always land here for the first edge-traversal as there is no previous connection traversed
            logger.debug("Incomplete traversal during computation of sub metrics for vehicle {} on connection {}", vehicleId, connectionId);
            return;
        }

        // Calculate Splines for the relation between distance driven and time/speed for the current traversal
        double[] distanceOffsets = new double[records.size()];
        double[] speeds = new double[records.size()];
        double[] timeStamps = new double[records.size()];
        // set initial values
        timeStamps[0] = (double) records.get(0).getTimeStamp();
        speeds[0] = records.get(0).getSpeed();
        distanceOffsets[0] = records.get(0).getOffset();
        // add offsets and timestamps
        for (int i = 1; i < records.size(); i++) {
            timeStamps[i] = (double) records.get(i).getTimeStamp();
            speeds[i] = records.get(i).getSpeed();
            distanceOffsets[i] = Math.max(records.get(i).getOffset(), distanceOffsets[i - 1] + 0.001);
        }
        // get the time-distance function to compute temporal mean speed
        PolynomialSplineFunction distanceTimeSpline = interpolateTimeDistanceFunction(distanceOffsets, timeStamps);
        PolynomialSplineFunction distanceSpeedSpline = interpolateSpeedDistanceFunction(distanceOffsets, speeds);

        double length = calculateTraversalLength(connectionId, distanceTimeSpline);

        try {
            double traversalTime = distanceTimeSpline.value(length) - distanceTimeSpline.value(0);
            double temporalMeanSpeed = (length / traversalTime) * TIME.SECOND;
            double spatialMeanSpeed = computeSpatialMeanSpeed(records, distanceSpeedSpline);
            float relativeMetric = -1;

            // insert RTSM as well, if thresholds are already available for the connection
            if (fcdDataStorage.gotThresholdFor(connectionId)) {
                relativeMetric = SpatioTemporalTrafficMetric.computeRelativeTrafficStatusMetric(
                        temporalMeanSpeed,
                        spatialMeanSpeed,
                        connectionId,
                        fcdDataStorage
                );
            }
            FcdRecord lastRecordOfTraversal = Iterables.getLast(traversal.getTraversal());
            fcdDataStorage.insertTraversalMetrics(
                    vehicleId,
                    lastRecordOfTraversal.getTimeStamp(),
                    connectionId,
                    traversal.getFollowingRecord() == null ? connectionId : traversal.getFollowingRecord().getConnectionId(),
                    spatialMeanSpeed,
                    temporalMeanSpeed,
                    computeNaiveTemporalMeanSpeed(records),
                    relativeMetric,
                    (long) traversalTime);
        } catch (OutOfRangeException e) {
            // catches errors using polynomial spline function, due to errors in the connections
            // length and the actual length a vehicle travels on it
            logger.error("Error during computeSubMetrics() for {} on Connection {}: {}", vehicleId, connectionId, e.getMessage());
        }
    }

    /**
     * This method build a list of records that is used to generate splines for a traversal. Additionally, the previous and following
     * records are considered with newly calculated offsets.
     * <br>
     * Note: Not exactly sure if we need the previous and following record
     *
     * @param traversal the traversal object
     * @return a {@link LinkedList} of {@link FcdRecord} representing a traversal
     */
    private LinkedList<FcdRecord> buildTraversalList(FcdTraversal traversal) {
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

    private double calculateTraversalLength(String connectionId, PolynomialSplineFunction distanceTimeSpline) {
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
     * Computes spatial-mean-speed by interpolating the speed over distance and averaging over equidistant points with
     * a distance of {@link #spatialMeanSpeedChunkSize}.
     *
     * @param records list of records of the connection-traversal
     * @return the spatial mean speed
     */
    private double computeSpatialMeanSpeed(LinkedList<FcdRecord> records, PolynomialSplineFunction distanceSpeedSpline) {
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
     * Interpolates using the recorded speeds, as the slope in a time-location plot will be
     * inaccurate for short connections with fewer records.
     *
     * @param distanceOffsets the distance driven between the i and the i + 1 record
     * @param speeds          the speed between the i and the i + 1 record
     * @return {@link PolynomialSplineFunction} representing the speed-distance function
     */
    private static PolynomialSplineFunction interpolateSpeedDistanceFunction(double[] distanceOffsets, double[] speeds) {
        LinearInterpolator linearInterpolator = new LinearInterpolator();
        return linearInterpolator.interpolate(distanceOffsets, speeds);
    }

    /**
     * Interpolates a time-distance function for the given records.
     *
     * @param distanceOffsets the distance driven between the i and the i + 1 record
     * @param timeStamps      the time between the i and the i + 1 record
     * @return PolynomialSplineFunction representing the time-distance function
     */
    private static PolynomialSplineFunction interpolateTimeDistanceFunction(double[] distanceOffsets, double[] timeStamps) {
        LinearInterpolator linearInterpolator = new LinearInterpolator();
        return linearInterpolator.interpolate(distanceOffsets, timeStamps);
    }

    /**
     * Computes the naive temporal-mean-speed by averaging speeds of the records.
     *
     * @param records to average their speeds
     * @return naive temporal mean speed
     */
    private double computeNaiveTemporalMeanSpeed(LinkedList<FcdRecord> records) {
        double sum = 0;
        int count = 0;
        String connectionId = records.get(1).getConnectionId();
        for (FcdRecord record : records) {
            if (record.getConnectionId().equals(connectionId)) {
                sum = sum + record.getSpeed();
                count++;
            }
        }
        return sum / count;
    }

    @Override
    public void initialize(UnitLogger logger) {
        this.logger = logger;
    }

    @Override
    public void shutdown(long shutdownTime) {
        // nop, we could potentially try to calculate metrics for the last connection
    }

    @Override
    public void onConnectionTraversal(String unitId, FcdTraversal traversal) {
        computeSubMetrics(unitId, traversal);
    }


    @Override
    public void withDataStorage(Database networkDatabase, FcdDataStorage fcdDataStorage) {
        this.networkDatabase = networkDatabase;
        this.fcdDataStorage = fcdDataStorage;
    }
}
