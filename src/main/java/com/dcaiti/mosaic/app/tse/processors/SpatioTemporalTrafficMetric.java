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

import com.dcaiti.mosaic.app.tse.data.TraversalStatistics;
import com.dcaiti.mosaic.app.tse.persistence.FcdDataStorage;

import org.apache.commons.math3.util.Pair;

import java.util.ArrayList;

/**
 * Holds multiple methods to compute all data for and around the relative traffic status metric (RTSM) by Yoon et al.
 */
public final class SpatioTemporalTrafficMetric {
    /**
     * Recomputes RTSM for the entire database, as thresholds may change.
     *
     * @param fcdDataStorage to get access to data from db
     */
    public static void recomputeRelativeTrafficMetricsForAllTraversals(FcdDataStorage fcdDataStorage) {
        ArrayList<TraversalStatistics> traversals = fcdDataStorage.getTraversalMetrics();
        traversals.forEach(traversal -> traversal.setRelativeTrafficStatusMetric(
                SpatioTemporalTrafficMetric.computeRelativeTrafficStatusMetric(
                        traversal.getTemporalMeanSpeed(), traversal.getSpatialMeanSpeed(), traversal.getConnectionId(), fcdDataStorage
                )
        ));
        fcdDataStorage.updateTraversalMetrics(traversals);
    }

    /**
     * Computes the relative traffic status metric proposed by Yoon et al.
     *
     * @param temporalMeanSpeed of data point
     * @param spatialMeanSpeed  of data point
     * @param connectionId      for which the metrics is needed
     * @param fcdDataStorage    to gather data from
     * @return a value between 0 and 1, or -1 if no thresholds have been computed yet.
     */
    public static float computeRelativeTrafficStatusMetric(Double temporalMeanSpeed, Double spatialMeanSpeed,
                                                           String connectionId, FcdDataStorage fcdDataStorage) {
        Pair<Double, Double> thresholds = fcdDataStorage.getThresholds(connectionId);
        if (thresholds == null) {
            return -1;
        }
        if (temporalMeanSpeed == -1 || spatialMeanSpeed == -1) {
            return 1;
        }
        return (float) (getDistanceBasedMetric(temporalMeanSpeed, spatialMeanSpeed, thresholds)
                / (thresholds.getFirst() + thresholds.getSecond()));
    }

    /**
     * Computes the (non-relative) distance-based traffic metric proposed by Yoon et al.
     *
     * @param temporalMeanSpeed of data point
     * @param spatialMeanSpeed  of data point
     * @param thresholds        representing the origin
     * @return distance-based traffic metric for the given data
     */
    private static double getDistanceBasedMetric(Double temporalMeanSpeed, Double spatialMeanSpeed, Pair<Double, Double> thresholds) {

        RtsmQuadrant quadrant = getQuadrant(temporalMeanSpeed, spatialMeanSpeed, thresholds);
        switch (quadrant) {
            case UpperRight:
                return 0;
            case UpperLeft:
                return (thresholds.getFirst() - temporalMeanSpeed);
            case LowerLeft:
                return (thresholds.getFirst() - temporalMeanSpeed) + (thresholds.getSecond() - spatialMeanSpeed);
            case LowerRight:
                return (thresholds.getSecond() - spatialMeanSpeed);
            default:
                return -1;
        }
    }

    /**
     * Finds the quadrant the given datapoint falls into.
     *
     * @param temporalMeanSpeed of datapoint
     * @param spatialMeanSpeed  of datapoint
     * @param thresholds        representing the origin
     * @return returns the calculated quadrant (see {@link RtsmQuadrant}.
     */
    private static RtsmQuadrant getQuadrant(Double temporalMeanSpeed, Double spatialMeanSpeed, Pair<Double, Double> thresholds) {
        boolean isAboveTemporal = temporalMeanSpeed >= thresholds.getFirst();
        boolean isAboveSpatial = spatialMeanSpeed >= thresholds.getSecond();
        if (isAboveSpatial) {
            if (isAboveTemporal) {
                return RtsmQuadrant.UpperRight;
            }
            return RtsmQuadrant.UpperLeft;
        } else {
            if (!isAboveTemporal) {
                return RtsmQuadrant.LowerLeft;
            }
            return RtsmQuadrant.LowerRight;
        }
    }

    /**
     * Enum to rank speed samples based on the quadrants build by the computed thresholds.
     * <ul>
     * <li>{@link RtsmQuadrant#UpperRight}: Spatial Mean Speed good, Temporal Mean Speed bad</li>
     * <li> {@link RtsmQuadrant#UpperLeft}: Spatial Mean Speed good, Temporal Mean Speed bad</li>
     * <li> {@link RtsmQuadrant#LowerLeft}: Spatial Mean Speed bad, Temporal Mean Speed bad</li>
     * <li> {@link RtsmQuadrant#LowerRight}: Spatial Mean Speed bad, Temporal Mean Speed good</li>
     * </ul>
     */
    public enum RtsmQuadrant {
        UpperRight, UpperLeft, LowerLeft, LowerRight
    }
}
